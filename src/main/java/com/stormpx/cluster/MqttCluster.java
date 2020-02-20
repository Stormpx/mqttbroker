package com.stormpx.cluster;

import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.VoteMessage;
import com.stormpx.cluster.net.*;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MqttCluster {
    private final static Logger logger= LoggerFactory.getLogger(MqttCluster.class);

    private Vertx vertx;

    private JsonObject config;

    private NetCluster netCluster;

    private StateHandler stateHandler;

    private ClusterState clusterState;

    private MemberType memberType=MemberType.FOLLOWER;

    private String leaderId;

    private Counter counter;

    private TimeoutStream timeoutStream;

    private Deque<ReadState> deque;
    private Map<String,Promise<Integer>> readIndexMap;

    public MqttCluster(Vertx vertx, JsonObject config) {
        this.clusterState=new ClusterState();
        this.vertx = vertx;
        this.config=config;
    }


    public Future<Void> start(){
        String nodeId = config.getString("id");
        if (nodeId==null){
            return Future.failedFuture("nodeId empty");
        }
        this.deque=new LinkedList<>();
        this.readIndexMap=new HashMap<>();

        return stateHandler.loadClusterState()
                .onSuccess(c->this.clusterState=c)
                .compose(v->{
                    this.clusterState=v;
                    this.clusterState.setId(nodeId);
                    this.netCluster=new NetClusterImpl(vertx,config);
                    return this.netCluster.appendEntriesRequestHandler(this::handleAppendEntriesRequest)
                            .appendEntriesResponseHandler(this::handleAppendEntriesResponse)
                            .voteRequestHandler(this::handleVoteRequest)
                            .voteResponseHandler(this::handleVoteResponse)
                            .requestHandler(stateHandler::handle)
                            .responseHandler(stateHandler::handle)
                            .readIndexRequestHandler(this::handleReadIndexRequest)
                            .requestIndexResponseHandler(this::handleReadIndexResponse)
                            .init();
                })
                .onSuccess(v-> becomeFollower(clusterState.getCurrentTerm()));

    }




    private void handleAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest){

        AppendEntriesMessage appendEntriesMessage =
                appendEntriesRequest.getAppendEntriesMessage();
        logger.debug("AppendEntriesRequest from node: {} term: {} currentTerm: {} ",appendEntriesMessage.getLeaderId(),appendEntriesMessage.getTerm(),clusterState.getCurrentTerm());
        int prevLogIndex = appendEntriesMessage.getPrevLogIndex();
        if (appendEntriesMessage.getTerm()<clusterState.getCurrentTerm()){
            //false
            appendEntriesRequest.response(clusterState.getCurrentTerm(),prevLogIndex,false);
            return;
        }

        this.leaderId=appendEntriesMessage.getLeaderId();

        boolean success=true;
        int requestLastIndex=prevLogIndex;

        LogEntry prevLog = clusterState.getLog(prevLogIndex);
        if (prevLogIndex !=0&& (prevLog ==null|| prevLog.getTerm()!=appendEntriesMessage.getPrevLogTerm())){
            //false
            success=false;
        }
        if (success) {
            List<LogEntry> logEntries = Optional.ofNullable(appendEntriesMessage.getEntries()).orElse(Collections.emptyList());
            for (LogEntry logEntry : logEntries) {
                LogEntry log = clusterState.getLog(logEntry.getIndex());
                if (log!=null){
                    //delete log
                    if (log.getTerm()!=logEntry.getTerm()){
                        int index = logEntry.getIndex();
                        int lastIndex = clusterState.getLastIndex();
                        if (index<lastIndex){
                            clusterState.setLastIndex(index);
                        }
                        stateHandler.delLog(index,lastIndex);
                        while (index<= lastIndex){
                            clusterState.delLog(index++);
                        }

                        clusterState.setLog(logEntry);
                        stateHandler.saveLog(logEntry);
                    }
                }else{
                    if (logEntry.getIndex()>clusterState.getLastIndex())
                        clusterState.setLastIndex(logEntry.getIndex());

                    clusterState.setLog(logEntry);
                    stateHandler.saveLog(logEntry);
                }

            }

            if (appendEntriesMessage.getLeaderCommit() > clusterState.getCommitIndex()) {
                int commitIndex=Math.min(appendEntriesMessage.getLeaderCommit(), logEntries.get(0)==null?Integer.MAX_VALUE: logEntries.get(0).getIndex());
                clusterState.setCommitIndex(commitIndex);

            }
            applyCommitIndex();

            stateHandler.firePending(appendEntriesMessage.getLeaderId());

            if (!logEntries.isEmpty())
                requestLastIndex= logEntries.get(logEntries.size()-1).getIndex();
        }
        becomeFollower(appendEntriesMessage.getTerm());



        //response
        appendEntriesRequest.response(clusterState.getCurrentTerm(),requestLastIndex,success);
    }

    private void handleAppendEntriesResponse(AppendEntriesResponse response){
        if (response.getTerm()>clusterState.getCurrentTerm()){
            becomeFollower(response.getTerm());
            if (memberType==MemberType.LEADER) {
                failState();
            }
            return;
        }
        if (memberType!=MemberType.LEADER)
            return;

        String nodeId = response.getNodeId();

        NodeState nodeState = netCluster.getNode(nodeId).state();
        if (!response.isSuccess()){
            nodeState.setNextIndex(nodeState.getNextIndex()-1);
        }else{
            if (response.getRequestLastIndex()>nodeState.getMatchIndex())
                nodeState.setMatchIndex(response.getRequestLastIndex());

            nodeState.setNextIndex(response.getRequestLastIndex()+1);

            List<Integer> list = netCluster.nodes().stream().map(ClusterNode::state).map(NodeState::getMatchIndex).sorted().collect(Collectors.toList());

            Integer matchIndex = list.get(list.size() / 2);

            if (matchIndex>clusterState.getCommitIndex()&&clusterState.getLog(matchIndex).getTerm()==clusterState.getCurrentTerm()){
                clusterState.setCommitIndex(matchIndex);
                applyCommitIndex();

                stateHandler.saveState(clusterState);
            }

            counter.add(nodeId);
            if (counter.isMajority()){
                this.counter=new Counter(netCluster.nodes().size());
                safeState();
                stateHandler.firePending(clusterState.getId());
            }

        }
    }

    private void handleVoteRequest(VoteRequest voteRequest){

        VoteMessage voteMessage = voteRequest.getVoteMessage();
        // if is connect test return true
        if (voteMessage.isPreVote()){
            voteRequest.response(new VoteResponse().setNodeId(clusterState.getId()).setVoteGranted(true).setTerm(clusterState.getCurrentTerm()));
            return;
        }
        logger.debug("vote request from node: {} term:{}",voteMessage.getCandidateId(),voteMessage.getTerm());
        if (voteMessage.getTerm()>clusterState.getCurrentTerm()) {
            this.leaderId=null;
            becomeFollower(voteMessage.getTerm());
        }
        boolean voteGranted=false;
        if ((voteMessage.getTerm()>=clusterState.getCurrentTerm()&&clusterState.getVotedFor()==null)
                &&((clusterState.getLastIndex()<=0||clusterState.getLog(clusterState.getLastIndex()).getTerm()<=voteMessage.getLastLogTerm())||
                (clusterState.getLog(clusterState.getLastIndex()).getTerm()==voteMessage.getLastLogTerm()&&voteMessage.getLastLogIndex()>=clusterState.getCommitIndex()))){

            clusterState.setVotedFor(voteMessage.getCandidateId());
            becomeFollower(voteMessage.getTerm());
            voteGranted=true;
        }
        voteRequest.response(new VoteResponse().setNodeId(clusterState.getId()).setVoteGranted(voteGranted).setTerm(clusterState.getCurrentTerm()));

    }
    private void handleVoteResponse(VoteResponse voteResponse){
        if (!voteResponse.isVoteGranted()){
            if (voteResponse.getTerm()>clusterState.getCurrentTerm()) {
                becomeFollower(voteResponse.getTerm());
            }
        }else{
            logger.debug("get vote from node:{}",voteResponse.getNodeId());
            if (memberType==MemberType.PRE_CANDIDATES||memberType==MemberType.CANDIDATES) {
                counter.add(voteResponse.getNodeId());
                if (counter.isMajority()) {
                    switch (memberType){
                        case PRE_CANDIDATES:
                            logger.debug("majority alive start vote");
                            becomeCandidates();
                            break;
                        case CANDIDATES:
                            logger.debug("node: {} term:{} memberType: {} election win! thanks myFamily and myFriend ",clusterState.getId(),clusterState.getCurrentTerm(),memberType.name());
                            becomeLeader();
                            break;
                    }
                }
            }
        }
    }


    private void handleReadIndexRequest(ReadIndexRequest readIndexRequest){
        if (this.memberType!=MemberType.LEADER){
            readIndexRequest.response(false,0);
            return;
        }
        readonlySafeFuture().setHandler(ar->{
           if (ar.succeeded()){
               readIndexRequest.response(true,ar.result());
           }else{
               readIndexRequest.response(false,0);
           }
        });

    }

    private void handleReadIndexResponse(ReadIndexResponse readIndexResponse){
        String id = readIndexResponse.getId();
        Promise<Integer> promise = readIndexMap.remove(id);
        if (!readIndexResponse.isLeader()){
            if (promise!=null)
                promise.tryFail("fail");
            return;
        }
        if (promise==null)
            return;
        int readIndex = readIndexResponse.getReadIndex();

        ReadState readState = new ReadState(readIndex);
        readState.promise.future().setHandler(ar->{
            if (ar.succeeded()){
                promise.tryComplete(readState.readIndex);
            }else{
                promise.tryFail(ar.cause());
            }
        });
        deque.addLast(readState);
    }


    private void applyCommitIndex(){
        int lastApplied = clusterState.getLastApplied();
        int commitIndex = clusterState.getCommitIndex();
        while (lastApplied < commitIndex){
            LogEntry log = clusterState.getLog(++lastApplied);
            if (log==null) {
                commitIndex=lastApplied;
                break;
            }
            stateHandler.executeLog(log);
        }
        clusterState.setCommitIndex(commitIndex);
        clusterState.setLastApplied(lastApplied);

    }

    private void safeState(){
        int lastApplied = clusterState.getLastApplied();
        while (!deque.isEmpty()){
            ReadState state = deque.peek();
            if (state.readIndex>lastApplied)
                break;

            state.promise.tryComplete(state.readIndex);

            deque.poll();

        }
    }

    private void failState(){
        Deque<ReadState> deque = this.deque;
        this.deque=new LinkedList<>();
        for (ReadState readState : deque) {
            readState.promise.tryFail("fail");
        }
    }

    /**
     * follower timer
     */
    private void setTimer(){
        if (this.timeoutStream!=null)
            this.timeoutStream.cancel();

        this.timeoutStream=vertx.timerStream(5000).handler(id->{
            this.leaderId=null;
            logger.debug("term: {} votedFor: {} commitIndex:{} lastApplied:{} lastIndex:{}",
                    clusterState.getCurrentTerm(),clusterState.getVotedFor(),clusterState.getCommitIndex(),
                    clusterState.getLastApplied(),clusterState.getLastIndex());
            if (memberType==MemberType.FOLLOWER||memberType==MemberType.PRE_CANDIDATES){
                becomePreCandidates();
            } else if (memberType==MemberType.CANDIDATES){
                becomeCandidates();
            }
        });
    }

    /**
     * leader timer
     */
    private void setLeaderTimer(){
        if (this.timeoutStream!=null)
            this.timeoutStream.cancel();

        this.timeoutStream=vertx.timerStream(2000).handler(id->{
           if (memberType==MemberType.LEADER){
               logger.debug("leader appendEntries term:{}",clusterState.getCurrentTerm());
               sendAppendEntries();
               setLeaderTimer();
           }else{
               logger.info("what happens");
           }
        });

    }

    private void becomeFollower(int term){
        this.memberType=MemberType.FOLLOWER;
        clusterState.setCurrentTerm(term);
        clusterState.setVotedFor(null);
        setTimer();
        this.counter =null;

        stateHandler.saveState(clusterState);
    }
    private void becomeLeader(){
        this.memberType=MemberType.LEADER;
        netCluster.initNodeIndex(clusterState.getLastIndex()+1);
        this.counter =new Counter(netCluster.nodes().size());
        this.leaderId=clusterState.getId();
        clusterState.setVotedFor(null);
        //add nop
        addLog(clusterState.getId(),0,null);
        //send heartbeat
        sendAppendEntries();
        //set timer
        setLeaderTimer();

        stateHandler.saveState(clusterState);
    }

    private void becomePreCandidates(){
        this.memberType=MemberType.PRE_CANDIDATES;
        this.counter =new Counter(netCluster.nodes().size());
        this.counter.add(clusterState.getId());
        sendVoteRequest(true);
        setTimer();

        stateHandler.saveState(clusterState);
    }

    private void becomeCandidates(){
        this.memberType=MemberType.CANDIDATES;
        clusterState.setCurrentTerm(clusterState.getCurrentTerm()+1);
        clusterState.setVotedFor(clusterState.getId());
        this.counter =new Counter(netCluster.nodes().size());
        this.counter.add(clusterState.getId());
        logger.debug("start vote term:{}",clusterState.getCurrentTerm());
        sendVoteRequest(false);
        setTimer();

        stateHandler.saveState(clusterState);
    }

    private void sendVoteRequest(boolean preVoteRequest){
        VoteMessage voteRequest = new VoteMessage();
        voteRequest.setPreVote(preVoteRequest);
        if (!preVoteRequest) {
            voteRequest.setTerm(clusterState.getCurrentTerm()).setCandidateId(clusterState.getId()).setLastLogIndex(clusterState.getLastIndex());
            if (clusterState.getLastIndex() <= 0) {
                voteRequest.setLastLogTerm(0);
            } else {
                voteRequest.setLastLogTerm(clusterState.getLog(clusterState.getLastIndex()).getTerm());
            }
        }
        netCluster.nodes().forEach(cn->netCluster.request(cn.id(),voteRequest));
    }

    private void sendAppendEntries(){
        netCluster.nodes().forEach(clusterNode -> {
            AppendEntriesMessage appendEntriesMessage = new AppendEntriesMessage()
                    .setTerm(clusterState.getCurrentTerm())
                    .setLeaderId(clusterState.getId())
                    .setLeaderCommit(clusterState.getCommitIndex());

            NodeState nodeState = clusterNode.state();
            logger.debug("node: {} log index range start:{} end:{}",nodeState.getNextIndex(),clusterState.getLastIndex());
            if (clusterState.getLastIndex()<=0){
                appendEntriesMessage.setPrevLogIndex(0);
                appendEntriesMessage.setPrevLogTerm(0);
            }else {
                int nextIndex = nodeState.getNextIndex();
                List<LogEntry> logs = IntStream
                        .range(nextIndex, clusterState.getLastIndex() + 1)
                        .boxed()
                        .map(clusterState::getLog)
                        .collect(Collectors.toList());

                appendEntriesMessage.setEntries(logs);
                if (nextIndex==1){
                    appendEntriesMessage.setPrevLogIndex(0);
                    appendEntriesMessage.setPrevLogTerm(0);
                }else {
                    appendEntriesMessage.setPrevLogIndex(nextIndex - 1);
                    appendEntriesMessage.setPrevLogTerm(clusterState.getLog(nextIndex - 1).getTerm());
                }
            }
            netCluster.request(clusterNode.id(),appendEntriesMessage);
        });

    }



    public Future<Integer> readonlySafeFuture(){
        if (this.memberType==MemberType.LEADER) {
            ReadState readState = deque.peekLast();
            if (readState != null) {
                if (readState.readIndex == clusterState.getCommitIndex())
                    return readState.promise.future();
            }
            ReadState state = new ReadState(clusterState.getCommitIndex());
            deque.addLast(state);
            return state.promise.future();
        }else{
            if (this.leaderId==null){
                return Future.failedFuture("no leader");
            }
            Promise<Integer> promise=Promise.promise();
            String id = UUID.randomUUID().toString();
            readIndexMap.put(id,promise);
            netCluster.requestReadIndex(leaderId,id);
            vertx.setTimer(1000,timer->{
                Promise<Integer> p = readIndexMap.remove(id);
                if (p!=null){
                    p.tryFail("timeout");
                }
            });
            return promise.future();
        }

    }



    public void addLog(String nodeId,int requestId,Buffer buffer){
        stateHandler.saveLog(clusterState.addLog(nodeId, requestId, buffer));
    }

    public void propose( int requestId, Buffer buffer){
        if (leaderId != null) {
            if (leaderId.equals(clusterState.getId())) {
                addLog(clusterState.getId(), requestId, buffer);
            } else {
                netCluster.request(clusterState.getId(),requestId,buffer);
            }
        }
    }

    public boolean isLeader(){
        return memberType==MemberType.FOLLOWER;
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public NetCluster net() {
        return netCluster;
    }

    public MemberType getMemberType() {
        return memberType;
    }


    class ReadState{
        private int readIndex;
        private Promise<Integer> promise;

        public ReadState(int readIndex) {
            this.readIndex = readIndex;
            this.promise=Promise.promise();
        }

    }

}
