package com.stormpx.cluster;

import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.RpcMessage;
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

    private Counter counter;



    private TimeoutStream timeoutStream;

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
        return stateHandler.loadClusterState()
                .onSuccess(c->this.clusterState=c)
                .compose(v->{
                    this.clusterState=v;
                    this.clusterState.setId(nodeId);
                    this.netCluster=new NetClusterImpl(vertx,config);
                    return this.netCluster.appendEntriesRequestHandler(this::onAppendEntriesRequest)
                            .voteRequestHandler(this::onVoteRequest)
                            .init();
                })
                .onSuccess(v->{
                    netCluster.nodes().forEach(cn->{
                        cn.appendEntriesResponseListener(this::onAppendEntriesResponse);
                        cn.voteResponseListener(this::onVoteResponse);
                    });

                    becomeFollower(clusterState.getCurrentTerm());


                   /* vertx.eventBus().<UnSafeJsonObject>localConsumer("_mqtt_message_dispatcher")
                            .handler(message->{
                                UnSafeJsonObject unSafeJsonObject = message.body();
                                JsonObject jsonObject = unSafeJsonObject.getJsonObject();
                                //todo
//                                sendToAllNode(jsonObject.toBuffer());

                            });

                    vertx.eventBus().<JsonObject>consumer("_mqtt_session_taken_over")
                            .handler(message->{
                                JsonObject body = message.body();
                                String clientId = body.getString("clientId");
                                String id = body.getString("id");
                                boolean sessionEnd = body.getBoolean("sessionEnd", false);
                                //todo
//                                sendToAllNode(Buffer.buffer());
                            });*/

                });

    }




    private void onAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest){

        AppendEntriesMessage appendEntriesMessage =
                appendEntriesRequest.getAppendEntriesMessage();
        logger.debug("AppendEntriesRequest from node: {} term: {} currentTerm: {} ",appendEntriesMessage.getLeaderId(),appendEntriesMessage.getTerm(),clusterState.getCurrentTerm());
        int prevLogIndex = appendEntriesMessage.getPrevLogIndex();
        if (appendEntriesMessage.getTerm()<clusterState.getCurrentTerm()){
            //false
            appendEntriesRequest.response(clusterState.getCurrentTerm(),prevLogIndex,false);
            return;
        }
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
                applyCommitIndex();
            }


            if (!logEntries.isEmpty())
                requestLastIndex= logEntries.get(logEntries.size()-1).getIndex();
        }
        becomeFollower(appendEntriesMessage.getTerm());

        stateHandler.onLeaderHeartbeat(appendEntriesMessage.getLeaderId());

        //response
        appendEntriesRequest.response(clusterState.getCurrentTerm(),requestLastIndex,success);
    }

    private void onAppendEntriesResponse(AppendEntriesResponse response){
        if (response.getTerm()>clusterState.getCurrentTerm()){
            becomeFollower(response.getTerm());
            return;
        }
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
                stateHandler.onLeaderHeartbeat(clusterState.getId());
            }

        }
    }

    private void onVoteRequest(VoteRequest voteRequest){

        VoteMessage voteMessage = voteRequest.getVoteMessage();
        // if is connect test return true
        if (voteMessage.isPreVote()){
            voteRequest.response(new VoteResponse().setNodeId(clusterState.getId()).setVoteGranted(true).setTerm(clusterState.getCurrentTerm()));
            return;
        }
        logger.debug("vote request from node: {} term:{}",voteMessage.getCandidateId(),voteMessage.getTerm());
        if (voteMessage.getTerm()>clusterState.getCurrentTerm()) {
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
    private void onVoteResponse(VoteResponse voteResponse){
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

    private void applyCommitIndex(){
        int lastApplied = clusterState.getLastApplied();
        int commitIndex = clusterState.getCommitIndex();
        while (lastApplied < commitIndex){
            stateHandler.executeLog(clusterState.getLog(++lastApplied));
        }
        clusterState.setLastApplied(lastApplied);

    }


    /**
     * follower timer
     */
    private void setTimer(){
        if (this.timeoutStream!=null)
            this.timeoutStream.cancel();

        this.timeoutStream=vertx.timerStream(5000).handler(id->{
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
        clusterState.setVotedFor(null);
        //add nop
        addLog(null);
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
        netCluster.nodes().forEach(cn->cn.request(voteRequest));
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
            clusterNode.request(appendEntriesMessage);
        });

    }
    private List<RpcMessage> pendingMessage;

    public void requestLeader(int requestId,Buffer payload){

    }

    public void request(List<String> nodeIds,int requestId, Buffer payload){


    }

    public void addLog(Buffer buffer){
        stateHandler.saveLog(clusterState.addLog(buffer));
    }

    public boolean isLeader(){
        return memberType==MemberType.FOLLOWER;
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public MemberType getMemberType() {
        return memberType;
    }
}
