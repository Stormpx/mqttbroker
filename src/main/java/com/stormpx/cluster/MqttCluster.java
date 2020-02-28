package com.stormpx.cluster;

import com.stormpx.cluster.message.*;
import com.stormpx.cluster.net.*;
import com.stormpx.kit.value.Values2;
import com.stormpx.store.ClusterDataStore;
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

    private ClusterDataStore clusterDataStore;

    private StateService stateService;

    private ClusterClient clusterClient;

    private ClusterState clusterState;

    private MemberType memberType=MemberType.FOLLOWER;

    private String leaderId;

    private Counter counter;

    private TimeoutStream timeoutStream;

    private Deque<ReadState> deque;
    private Map<String,ReadState> readIndexMap;


    public MqttCluster(Vertx vertx, JsonObject config, ClusterDataStore clusterDataStore, StateService stateService, ClusterClient clusterClient) {
        this.vertx = vertx;
        this.config = config;
        this.clusterDataStore = clusterDataStore;
        this.stateService = stateService;
        this.clusterClient = clusterClient;
    }

    public Future<Void> start(){
        String nodeId = config.getString("id");
        if (nodeId==null){
            return Future.failedFuture("nodeId empty");
        }
        this.deque=new LinkedList<>();
        this.readIndexMap=new HashMap<>();
        vertx.eventBus().registerDefaultCodec(LogEntry.class,LogEntry.CODEC);
        return clusterClient.init(this)
                .compose(v->stateService.init(this))
                .compose(v->loadClusterState(nodeId))
                .compose(v->{
                    this.netCluster=new NetClusterImpl(vertx,config);
                    return this.netCluster.appendEntriesRequestHandler(this::handleAppendEntriesRequest)
                            .appendEntriesResponseHandler(this::handleAppendEntriesResponse)
                            .voteRequestHandler(this::handleVoteRequest)
                            .voteResponseHandler(this::handleVoteResponse)
                            .requestHandler(this::handleRequest)
                            .responseHandler(this::handleResponse)
                            .readIndexRequestHandler(this::handleReadIndexRequest)
                            .requestIndexResponseHandler(this::handleReadIndexResponse)
                            .init();
                })
                .onSuccess(v-> becomeFollower(clusterState.getCurrentTerm()));

    }

    private Future<ClusterState> loadClusterState(String id) {
        return clusterDataStore.getState()
                .compose(state-> clusterDataStore.logs().map(list-> Values2.values(state,list)))
                .map(v->{
                    JsonObject state = v.getOne();
                    List<LogEntry> logEntryList = v.getTwo();
                    this.clusterState = new ClusterState();
                    clusterState.setId(id);
                    if (state!=null){
                        clusterState.setCurrentTerm(state.getInteger("term"));
                        clusterState.setLastIndex(state.getInteger("lastIndex"));
                        clusterState.setCommitIndex(state.getInteger("commitIndex"));
                    }
                    if (logEntryList!=null){
                        int commitIndex = clusterState.getCommitIndex();
                        logEntryList.stream()
                                .sorted(Comparator.comparingInt(LogEntry::getIndex))
                                .forEachOrdered(log->{
                                    clusterState.setLog(log);
                                    if (log.getIndex()<=commitIndex) {
                                        stateService.applyLog(log);
                                    }
                                });
                        clusterState.setLastApplied(commitIndex);
                    }

                    return clusterState;
                });
    }

    private void handleRequest(Request request){
        stateService.handle(request.getRpcMessage())
                .setHandler(ar->{
                   if (ar.succeeded()){
                       Response response = ar.result();
                       request.response(response.isSuccess(),response.getPayload());
                   }else{
                       request.response(false,null);
                   }
                });
    }

    private void handleResponse(Response response){
        clusterClient.fire(response.getRequestId(),response);
    }

    private void handleAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest){
        AppendEntriesMessage appendEntriesMessage =
                appendEntriesRequest.getAppendEntriesMessage();
        logger.debug("AppendEntriesRequest from node: {} term: {} currentTerm: {} leaderCommitIndex :{} currentCommitIndex:{}  ",
                appendEntriesMessage.getLeaderId(),appendEntriesMessage.getTerm(),clusterState.getCurrentTerm(),appendEntriesMessage.getLeaderCommit(),clusterState.getCommitIndex(),
                appendEntriesMessage.getLeaderCommit(),clusterState.getCommitIndex());
        try {
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
                            clusterDataStore.delLog(index,lastIndex);
                            while (index<= lastIndex){
                                clusterState.delLog(index++);
                            }

                            clusterState.setLog(logEntry);
                            clusterDataStore.saveLog(logEntry);
                        }
                    }else{
                        //new log
                        if (logEntry.getIndex()>clusterState.getLastIndex())
                            clusterState.setLastIndex(logEntry.getIndex());

                        clusterState.setLog(logEntry);

                        clusterDataStore.saveLog(logEntry);
                    }

                }

                if (appendEntriesMessage.getLeaderCommit() > clusterState.getCommitIndex()) {
                    int commitIndex=Math.min(appendEntriesMessage.getLeaderCommit(),logEntries.isEmpty()?Integer.MAX_VALUE: logEntries.get(0).getIndex());
                    clusterState.setCommitIndex(commitIndex);

                }
                applyCommitIndex();

                fireReadIndex();
                stateService.firePendingEvent(appendEntriesMessage.getLeaderId());

                if (!logEntries.isEmpty())
                    requestLastIndex= logEntries.get(logEntries.size()-1).getIndex();
            }
            becomeFollower(appendEntriesMessage.getTerm());


            //response
            appendEntriesRequest.response(clusterState.getCurrentTerm(),requestLastIndex,success);
        } catch (Exception e) {
            //false
            appendEntriesRequest.response(clusterState.getCurrentTerm(),appendEntriesMessage.getPrevLogIndex(),false);
            logger.error("handle appendEntriesRequest failed ",e);
        }
    }

    private void handleAppendEntriesResponse(AppendEntriesResponse response){
        logger.debug("response from node:{} success:{} term:{} lastIndex:{} currentTrem:{}",
                response.getNodeId(),response.isSuccess(),response.getTerm(),response.getRequestLastIndex(),clusterState.getCurrentTerm());

        if (response.getTerm()>clusterState.getCurrentTerm()){
            becomeFollower(response.getTerm());
            this.leaderId=null;
            if (memberType==MemberType.LEADER) {
                failReadIndex();
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

                saveSate();
            }

            counter.add(nodeId);
            if (counter.isMajority()){
                this.counter=new Counter(netCluster.nodes().size());
                this.counter.add(clusterState.getId());
                fireReadIndex();
                stateService.firePendingEvent(clusterState.getId());
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
        readIndex().setHandler(ar->{
           if (ar.succeeded()){
               readIndexRequest.response(true,ar.result());
           }else{
               readIndexRequest.response(false,0);
           }
        });

    }

    private void handleReadIndexResponse(ReadIndexResponse readIndexResponse){
        String id = readIndexResponse.getId();
        ReadState readState = readIndexMap.remove(id);
        if (readState==null)
            return;
        Promise<Integer> promise = readState.promise;
        if (!readIndexResponse.isLeader()){
            if (promise!=null)
                promise.tryFail("fail");
            return;
        }

        int readIndex = readIndexResponse.getReadIndex();
        readState.readIndex(readIndex);

        deque.addLast(readState);
        fireReadIndex();
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
            stateService.applyLog(log);
        }
        clusterState.setCommitIndex(commitIndex);
        clusterState.setLastApplied(lastApplied);

    }

    private void fireReadIndex(){
        int lastApplied = clusterState.getLastApplied();
        Iterator<ReadState> iterator = deque.iterator();
        while (iterator.hasNext()){

            ReadState readState = iterator.next();
            if (readState.readIndex>lastApplied){
                continue;
            }
            readState.promise.tryComplete(readState.readIndex);
            iterator.remove();
        }
    }

    private void failReadIndex(){
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

        saveSate();
    }
    private void becomeLeader(){
        this.memberType=MemberType.LEADER;
        netCluster.initNodeIndex(clusterState.getLastIndex()+1);
        this.counter =new Counter(netCluster.nodes().size());
        this.counter.add(clusterState.getId());
        this.leaderId=clusterState.getId();
        clusterState.setVotedFor(null);
        //add nop
        addLog(clusterState.getId(),0,null);
        clusterState.markTermFirstIndex();
        //send heartbeat
        sendAppendEntries();
        //set timer
        setLeaderTimer();

        saveSate();
    }

    private void becomePreCandidates(){
        this.memberType=MemberType.PRE_CANDIDATES;
        this.counter =new Counter(netCluster.nodes().size());
        this.counter.add(clusterState.getId());
        sendVoteRequest(true);
        setTimer();

        saveSate();
    }

    private void becomeCandidates(){
        this.memberType=MemberType.CANDIDATES;
        clusterState.setCurrentTerm(clusterState.getCurrentTerm()+1);
        clusterState.setVotedFor(clusterState.getId());
        this.counter =new Counter(netCluster.nodes().size());
        this.counter.add(clusterState.getId());
        logger.debug("start vote term: {}",clusterState.getCurrentTerm());
        sendVoteRequest(false);
        setTimer();

        saveSate();
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
            logger.debug("node: {} log index range start:{} end: {}",clusterNode.id(),nodeState.getNextIndex(),clusterState.getLastIndex());
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



    public Future<Integer> readIndex(){
        String id = UUID.randomUUID().toString();
        if (this.memberType==MemberType.LEADER) {
            ReadState readState = deque.peekLast();
            if (readState != null) {
                if (readState.readIndex == clusterState.readIndex())
                    return readState.promise.future();
            }
            ReadState state = new ReadState(id);
            state.readIndex(clusterState.readIndex());
            deque.addLast(state);

            return state.promise.future();
        }else{
            if (this.leaderId==null){
                return Future.failedFuture("no leader");
            }
            ReadState readState = new ReadState(id);
            readState.setTimer();

            readIndexMap.put(id,readState);

            netCluster.requestReadIndex(leaderId,id);

            return readState.promise.future();
        }

    }

    private void saveSate(){
        JsonObject json = new JsonObject();
        json.put("term",clusterState.getCurrentTerm());
        json.put("lastIndex",clusterState.getLastIndex());
        json.put("commitIndex",clusterState.getCommitIndex());
        json.put("lastApplied",clusterState.getLastApplied());
        clusterDataStore.saveState(json);
    }

    public void addLog(String nodeId,int requestId,Buffer buffer){
        clusterDataStore.saveLog(clusterState.addLog(nodeId, requestId, buffer));
    }


    public boolean isLeader(){
        return memberType==MemberType.FOLLOWER;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public String id(){
        return clusterState.getId();
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
        private String id;
        private Integer readIndex;
        private Promise<Integer> promise;
        private TimeoutStream timeoutStream;


        public ReadState(String id) {
            this.id=id;
            this.promise=Promise.promise();
        }

        public ReadState readIndex(Integer readIndex){
            if (this.timeoutStream!=null)
                this.timeoutStream.cancel();

            this.readIndex = readIndex;
            return this;
        }

        public void setTimer(){
            this.timeoutStream=vertx.timerStream(5100);
            this.timeoutStream.handler(id->{
                readIndexMap.remove(this.id);
                logger.debug("readIndex timeout id:{}",this.id);
                if (promise!=null){
                    promise.tryFail("timeout");
                }
            });
            this.promise.future().onComplete(v->{
                if (this.timeoutStream!=null)
                    this.timeoutStream.cancel();
            });
        }
    }

}
