package com.stormpx.cluster;

import com.stormpx.Constants;
import com.stormpx.cluster.message.*;
import com.stormpx.cluster.mqtt.ClusterClient;
import com.stormpx.cluster.net.*;
import com.stormpx.cluster.snapshot.*;
import com.stormpx.store.ClusterDataStore;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class MqttCluster {
    private final static Logger logger= LoggerFactory.getLogger(MqttCluster.class);

    private Vertx vertx;

    private JsonObject config;

    private NetCluster netCluster;

    private ClusterDataStore clusterDataStore;

    private StateService stateService;

    private ClusterClient clusterClient;

    private ClusterState clusterState;

    private LogList logList;
    private Snapshot snapshot;
    private boolean snapshotInstalling;

    private MemberType memberType=MemberType.FOLLOWER;

    private String leaderId;

    private Counter counter;

    private TimeoutStream timeoutStream;

    private Deque<ReadState> deque;
    private ReadState pendingReadState;
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
        return clusterClient.init(this)
                .compose(v->stateService.init(this))
                .compose(v->loadClusterState(nodeId))
                .compose(v->{
                    this.netCluster=new NetClusterImpl(vertx,config);
                    return this.netCluster.appendEntriesRequestHandler(this::handleAppendEntriesRequest)
                            .appendEntriesResponseHandler(this::handleAppendEntriesResponse)
                            .installSnapshotRequestHandler(this::handleInstallRequestHandler)
                            .installSnapshotResponseHandler(this::handleInstallResponseHandler)
                            .voteRequestHandler(this::handleVoteRequest)
                            .voteResponseHandler(this::handleVoteResponse)
                            .requestHandler(this::handleRequest)
                            .responseHandler(this::handleResponse)
                            .readIndexRequestHandler(this::handleReadIndexRequest)
                            .requestIndexResponseHandler(this::handleReadIndexResponse)
                            .init();
                })
                .onSuccess(v-> becomeFollower(clusterState.getCurrentTerm(),false));

    }




    private Future<Void> loadSnapshot(){
        Promise<Void> promise=Promise.promise();
        clusterDataStore.getSnapshotMeta()
                .compose(meta->{
                    String nodeId=clusterState.getId();
                    int lastIndex=0;
                    int lastTerm=0;
                    if (meta!=null){
                        nodeId=meta.getNodeId();
                        lastIndex=meta.getIndex();
                        lastTerm=meta.getTerm();
                    }
                    logger.debug("snapshot lastIncludeIndex:{} lastIncludeTerm:{}",lastIndex,lastTerm);
                    this.snapshot=new Snapshot(vertx,nodeId,lastIndex,lastTerm,config.getString(Constants.SAVE_DIR));
                    snapshot.snapshotHandler(this::tryInstallSnapshot);
                    if (lastIndex!=0){
                        return snapshot.reader(clusterState.getId())
                                .compose(reader->stateService.applySnapshot(reader).onSuccess(v->reader.done()));

                    }
                    return Future.succeededFuture();
                })
                .map((Void)null)
                .setHandler(promise);

        return promise.future();
    }

    public Future<Void> loadLogs(){
        Promise<Void> promise=Promise.promise();
        clusterDataStore.getIndex()
                .onFailure(promise::tryFail)
                .onSuccess(json->{
                    int lastLogIndex=0;
                    int firstLogIndex=snapshot.meta().getIndex();
                    if (json!=null){
                        lastLogIndex=json.getInteger("lastIndex");
                    }else{
                        lastLogIndex=firstLogIndex;
                    }

                    int finalLastLogIndex = lastLogIndex;

                    clusterDataStore.getLogs(firstLogIndex,lastLogIndex+1)
                            .onFailure(promise::tryFail)
                            .onSuccess(logEntryList->{
                                if (logEntryList!=null){
                                    int commitIndex = clusterState.getCommitIndex();
                                    logEntryList.sort(Comparator.comparingInt(LogEntry::getIndex));
                                    List<LogEntry> cacheList=new ArrayList<>();
                                    for (LogEntry logEntry : logEntryList) {
                                        if (logEntry.getIndex()<=commitIndex) {
                                            stateService.applyLog(logEntry);
                                        }
                                        if (logEntry.getIndex()>=commitIndex){
                                            cacheList.add(logEntry);
                                        }
                                    }
                                    clusterState.setLastApplied(commitIndex);
                                    this.logList=new LogList(clusterDataStore, firstLogIndex, finalLastLogIndex);
                                    for (LogEntry logEntry : cacheList) {
                                        this.logList.setLog(logEntry,false);
                                    }
                                    clusterState.setLogList(logList);
                                }
                                promise.tryComplete();

                            });
                });

        return promise.future();
    }

    private Future<Void> loadClusterState(String id) {
        return clusterDataStore.getState()
                .compose(state->{
                    this.clusterState = new ClusterState();
                    clusterState.setId(id);

                    if (state!=null){
                        clusterState.setCurrentTerm(state.getInteger("term"));
                        clusterState.setCommitIndex(state.getInteger("commitIndex"));

                    }
                    return Future.succeededFuture();
                })
                .compose(v->loadSnapshot())
                .compose(v->loadLogs());
    }

    private void tryInstallSnapshot(SnapshotMeta meta){
        int index = meta.getIndex();
        logList.truncatePrefix(index);
        clusterDataStore.saveSnapshotMeta(meta);
        SnapshotMeta snapshotMeta = this.snapshot.meta();
        if (!meta.getNodeId().equals(clusterState.getId())){
            // install snapshot
            snapshotInstalling =true;
            this.snapshot.reader(clusterState.getId())
                .onFailure(t-> {
                    logger.error("get snapshot reader failed",t);
                    snapshotInstalling =false;
                })
                .onSuccess(reader->stateService.applySnapshot(reader).onComplete(ar->{
                    if (ar.failed())
                        logger.error("apply snapshot failed",ar.cause());
                    else {
                        logger.info("install snapshot success nodeId:{} index:{} term:{}",
                                snapshotMeta.getNodeId(),snapshotMeta.getIndex(),snapshotMeta.getTerm());
                        clusterState.setCommitIndex(snapshotMeta.getIndex());
                        clusterState.setLastApplied(snapshotMeta.getIndex());
                    }

                    snapshotInstalling =false;

                    reader.done();

                }))
            ;
        }
    }

    private void logCompact(){
        if (!snapshot.snapshotting()&&clusterState.getLastApplied()-snapshot.meta().getIndex()>clusterState.getCompactInterval()){
            //compact
            logger.debug("create new snapshot index:{} term:{}",clusterState.getLastApplied(),clusterState.getCurrentTerm());
            SnapshotContext writerContext = snapshot.createWriterContext(clusterState.getId(), clusterState.getLastApplied(), clusterState.getCurrentTerm());
            stateService.writeSnapshot(writerContext);
        }

    }

    private void handleRequest(ClientExtendRequest request){
        stateService.handle(request);
    }

    private void handleResponse(Response response){
        clusterClient.fireResponse(response.getRequestId(),response);
    }


    private void handleInstallRequestHandler(InstallSnapshotRequest installSnapshotRequest) {
        InstallSnapshotMessage installSnapshotMessage = installSnapshotRequest.getInstallSnapshotMessage();
        if (installSnapshotMessage.getTerm()<clusterState.getCurrentTerm()){
            installSnapshotRequest.response(false,false,0,clusterState.getCurrentTerm());
            return;
        }

        becomeFollower(installSnapshotMessage.getTerm(),true);

        if (installSnapshotMessage.getLastIncludeIndex()<=clusterState.getCommitIndex()){
            installSnapshotRequest.response(true,true,0,clusterState.getCurrentTerm());
            return;
        }

        if (snapshot.snapshotting()){
            SnapshotContext snapshotContext = snapshot.writerContext();

            if (snapshotContext.getSnapshotMeta().getNodeId().equals(installSnapshotMessage.getLeaderId())){
                snapshotContext.getWriter()
                        .onSuccess(snapshotWriter->{
                            logger.debug("receive snapshot chunk current offset:{}  offset:{} write ",snapshotWriter.getOffset(),installSnapshotMessage.getOffset());
                            if (snapshotWriter.isEnd()){
                                installSnapshotRequest.response(true,false,installSnapshotMessage.getOffset(),clusterState.getCurrentTerm());
                                return;
                            }
                            if (installSnapshotMessage.getOffset()==snapshotWriter.getOffset()){

                                snapshotWriter.write(installSnapshotMessage.getBuffer());
                                if (installSnapshotMessage.isDone()){
                                    snapshotWriterDone(installSnapshotRequest,snapshotWriter);
                                    return;
                                }
                            }
                            installSnapshotRequest.response(true,false,snapshotWriter.getOffset(),clusterState.getCurrentTerm());
                        });

            }else{
                // compare index if index newer than current snapshotWriter index replace
                logger.debug("snapshotting id:{} requset leaderId:{}",snapshotContext.getSnapshotMeta().getNodeId(),installSnapshotMessage.getLeaderId());
                snapshotContext.getWriter()
                        .onSuccess(snapshotWriter->{
                            if (installSnapshotMessage.getLastIncludeIndex()<snapshotWriter.getSnapshotMeta().getIndex()){
                                installSnapshotRequest.response(false,false,0,clusterState.getCurrentTerm());
                                return;
                            }

                            if (installSnapshotMessage.getOffset()!=0){
                                installSnapshotRequest.response(true,false,0,clusterState.getCurrentTerm());
                                return;
                            }

                            startWriter(installSnapshotRequest);
                        });
            }
        }else {
            startWriter(installSnapshotRequest);
        }
    }

    private void startWriter(InstallSnapshotRequest installSnapshotRequest){
        InstallSnapshotMessage installSnapshotMessage = installSnapshotRequest.getInstallSnapshotMessage();
        snapshot.createWriterContext(installSnapshotMessage.getLeaderId(), installSnapshotMessage.getLastIncludeIndex(), installSnapshotMessage.getLastIncludeTerm())
                .getWriter()
                .onFailure(t -> installSnapshotRequest.response(false,false, 0, clusterState.getCurrentTerm()))
                .onSuccess(writer -> {
                    writer.write(installSnapshotMessage.getBuffer());
                    if (installSnapshotMessage.isDone()){
                        snapshotWriterDone(installSnapshotRequest,writer);
                        return;
                    }
                    installSnapshotRequest.response(true,false,writer.getOffset(),clusterState.getCurrentTerm());

                });
    }

    private void snapshotWriterDone(InstallSnapshotRequest installSnapshotRequest,SnapshotWriter snapshotWriter){
        snapshotWriter.end()
                .onFailure(t->{
                    logger.error("try finish writer failed",t);
                    installSnapshotRequest.response(true,false,0,clusterState.getCurrentTerm());
                })
                .onSuccess(v->{
                    installSnapshotRequest.response(true,true,snapshotWriter.getOffset(),clusterState.getCurrentTerm());
                });
    }



    private void handleInstallResponseHandler(InstallSnapshotResponse installSnapshotResponse) {
        if (logger.isDebugEnabled())
            logger.debug("install snapshot response {}", Json.encode(installSnapshotResponse));

        if (memberType!=MemberType.LEADER){
            return;
        }
        String nodeId = installSnapshotResponse.getNodeId();
        ClusterNode clusterNode = netCluster.getNode(nodeId);
        if (clusterNode==null){
            logger.debug("node:{} not found",nodeId);
            return;
        }
        NodeState nodeState = clusterNode.state();
        if (installSnapshotResponse.isAccept()){

            if (installSnapshotResponse.isDone()){
                snapshot.readDone(nodeId);
                nodeState.setMatchIndex(snapshot.meta().getIndex());
                nodeState.setNextIndex(snapshot.meta().getIndex()+1);
                return;
            }
            snapshot.reader(nodeId)
                    .onFailure(t->logger.error("get snapshot reader failed",t))
                    .onSuccess(reader->{
                        reader.setOffset(installSnapshotResponse.getNextOffset());
                        sendChunk(clusterNode,reader);
                    });

        }else{
            logger.debug("node:{} reject snapshot install term:{} currentTerm:{} snapshot index:{} snapshot term:{} nextIndex:{}",
                    nodeId,installSnapshotResponse.getTerm(),clusterState.getCurrentTerm(),snapshot.meta().getIndex(),snapshot.meta().getTerm(),nodeState.getNextIndex());
            if (installSnapshotResponse.getTerm()>clusterState.getCurrentTerm()){
                becomeFollower(installSnapshotResponse.getTerm(),true);
            }else{
                nodeState.setNextIndex(snapshot.meta().getIndex()+1);
            }
            snapshot.readDone(nodeId);


        }

    }



    private void handleAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest){
        AppendEntriesMessage appendEntriesMessage =
                appendEntriesRequest.getAppendEntriesMessage();
        logger.debug("AppendEntriesRequest from node: {} term: {} currentTerm: {} prevLogIndex:{} prevLogTerm:{} leaderCommitIndex :{} currentCommitIndex:{}  ",
                appendEntriesMessage.getLeaderId(),appendEntriesMessage.getTerm(),clusterState.getCurrentTerm(),appendEntriesMessage.getPrevLogIndex(),appendEntriesMessage.getPrevLogTerm(),appendEntriesMessage.getLeaderCommit(),clusterState.getCommitIndex(),
                appendEntriesMessage.getLeaderCommit(),clusterState.getCommitIndex());
        try {
            int prevLogIndex = appendEntriesMessage.getPrevLogIndex();
            if (appendEntriesMessage.getTerm()<clusterState.getCurrentTerm()){
                //false
                appendEntriesRequest.response(clusterState.getCurrentTerm(),prevLogIndex,false);
                return;
            }
            if (this.memberType==MemberType.LEADER){
                fireReadIndex();
                failReadIndex();
            }
            becomeFollower(appendEntriesMessage.getTerm(),false);

            this.leaderId=appendEntriesMessage.getLeaderId();

            List<LogEntry> logEntries = Optional.ofNullable(appendEntriesMessage.getEntries())
                    .map(logs->{
                        logs.sort(Comparator.comparingInt(LogEntry::getIndex));
                        return logs;
                    })
                    .orElse(Collections.emptyList());

            if (logList.getLastLogIndex()<prevLogIndex){
                appendEntriesRequest.response(clusterState.getCurrentTerm(),prevLogIndex,false);
                return;
            }
            if (prevLogIndex==0){
                logList.truncateSuffix(0);
                appendEntriesRequest.response(clusterState.getCurrentTerm(),appendLog(appendEntriesMessage,logEntries),true);
            }else {
                //get prevLogIndex --- prevLogIndex+1
                logList.getLog(prevLogIndex, prevLogIndex + 2).onFailure(t -> {
                    appendEntriesRequest.response(clusterState.getCurrentTerm(), appendEntriesMessage.getPrevLogIndex(), false);
                    logger.error("get log index:{} failed", t, prevLogIndex);
                }).onSuccess(logs -> {

                    if (logs.isEmpty()&&snapshot.meta().getIndex()!=prevLogIndex) {
                        logger.debug("prevIndex:{} snapshot lastIndex:{}",prevLogIndex,snapshot.meta().getIndex());
                        appendEntriesRequest.response(clusterState.getCurrentTerm(), appendEntriesMessage.getPrevLogIndex(), false);
                        return;
                    }
                    int currentPrevLogIndex=0;
                    int currentPrevLogTerm=0;
                    if (!logs.isEmpty()) {
                        LogEntry prevLog = logs.get(0);
                        currentPrevLogIndex=prevLog.getIndex();
                        currentPrevLogTerm=prevLog.getTerm();
                    }else{
                        currentPrevLogIndex=snapshot.meta().getIndex();
                        currentPrevLogTerm=snapshot.meta().getTerm();
                    }
                    logger.debug("currentPrevLogIndex:{} prevLogIndex:{} currentPrevLogTerm:{} prevLogTerm:{}",
                            currentPrevLogIndex,prevLogIndex,currentPrevLogTerm,appendEntriesMessage.getPrevLogTerm());
                    if (currentPrevLogIndex != prevLogIndex || currentPrevLogTerm != appendEntriesMessage.getPrevLogTerm()) {
                        appendEntriesRequest.response(clusterState.getCurrentTerm(), appendEntriesMessage.getPrevLogIndex(), false);
                        return;
                    }

                    if (logs.size() > 1 && !logEntries.isEmpty()) {
                        LogEntry logEntry = logs.get(logs.size() - 1);
                        LogEntry newLogEntry = logEntries.get(0);
                        if (logEntry.getTerm() != newLogEntry.getTerm())
                            logList.truncateSuffix(newLogEntry.getIndex());
                    }

                    appendEntriesRequest.response(clusterState.getCurrentTerm(), appendLog(appendEntriesMessage, logEntries), true);

                });
            }

        } catch (Exception e) {
            //false
            appendEntriesRequest.response(clusterState.getCurrentTerm(),appendEntriesMessage.getPrevLogIndex(),false);
            logger.error("handle appendEntriesRequest failed ",e);
        }
    }

    private int appendLog(AppendEntriesMessage appendEntriesMessage,List<LogEntry> logEntries){
        for (LogEntry logEntry : logEntries) {
            logList.setLog(logEntry,true);
        }

        if (appendEntriesMessage.getLeaderCommit() > clusterState.getCommitIndex()) {
            int commitIndex=Math.min(appendEntriesMessage.getLeaderCommit(),logEntries.isEmpty()?Integer.MAX_VALUE: logEntries.get(0).getIndex());
            clusterState.setCommitIndex(commitIndex);

        }

        applyCommitIndex()
                .onFailure(t->logger.error("applyCommitIndex failed",t))
                .onSuccess(v->{

                    fireReadIndex();
                    stateService.firePendingEvent(appendEntriesMessage.getLeaderId());
                    logList.releasePrefix(clusterState.getLastApplied()-1);

                })
                .onComplete(v->saveSate());

        int requestLastIndex= appendEntriesMessage.getPrevLogIndex();
        if (!logEntries.isEmpty())
            requestLastIndex= logEntries.get(logEntries.size()-1).getIndex();

        //response
        return requestLastIndex;
    }

    private void handleAppendEntriesResponse(AppendEntriesResponse response){
        logger.debug("response from node:{} success:{} term:{} lastIndex:{} currentTerm:{}",
                response.getNodeId(),response.isSuccess(),response.getTerm(),response.getRequestLastIndex(),clusterState.getCurrentTerm());

        if (response.getTerm()>clusterState.getCurrentTerm()){
            becomeFollower(response.getTerm(),true);
            this.leaderId=null;
            if (memberType==MemberType.LEADER) {
                fireReadIndex();
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

            logList.getLog(matchIndex)
                    .onFailure(t->logger.error("get matchIndex log failed",t))
                    .compose(log->{
                        if (matchIndex>clusterState.getCommitIndex()&&log.getTerm()==clusterState.getCurrentTerm()){
                            clusterState.setCommitIndex(matchIndex);
                            return applyCommitIndex()
                                    .onFailure(t->logger.error("applyCommitIndex failed",t))
                                    .onComplete(v->{
                                        saveSate();
                                        Integer index = list.get(0);
                                        logList.releasePrefix(index);
                                    });

                        }
                        return Future.succeededFuture();
                    })
                    .onComplete(v->{
                        counter.add(nodeId);
                        if (counter.isMajority()){
                            this.counter=new Counter(netCluster.nodes().size());
                            this.counter.add(clusterState.getId());
                            fireReadIndex();
                            stateService.firePendingEvent(clusterState.getId());
                        }
                    });

        }
    }

    private void handleVoteRequest(VoteRequest voteRequest){

        VoteMessage voteMessage = voteRequest.getVoteMessage();
        // if connect test return true
        if (voteMessage.isPreVote()){
            voteRequest.response(new VoteResponse().setNodeId(clusterState.getId()).setPreVote(true).setVoteGranted(true).setTerm(clusterState.getCurrentTerm()));
            return;
        }
        logger.debug("vote request from node: {} term:{}",voteMessage.getCandidateId(),voteMessage.getTerm());
        if (voteMessage.getTerm()>clusterState.getCurrentTerm()) {
            this.leaderId=null;
            becomeFollower(voteMessage.getTerm(),true);
        }
        logList.getLastLog()
                .onFailure(t->{
                    logger.error("get last log failed",t);
                    voteRequest.response(new VoteResponse().setNodeId(clusterState.getId()).setVoteGranted(false).setTerm(clusterState.getCurrentTerm()));
                })
                .onSuccess(lastLog->{
                    try {
                        boolean voteGranted=false;
                        int lastLogIndex=logList.getLastLogIndex();
                        int lastLogTerm=0;
                        if (logList.getLastLogIndex()==snapshot.meta().getIndex()){
                            lastLogTerm=snapshot.meta().getTerm();
                        }
                        if ((voteMessage.getTerm()>=clusterState.getCurrentTerm()&&clusterState.getVotedFor()==null)
                                &&((lastLogIndex<=0||lastLogTerm<voteMessage.getLastLogTerm())
                                ||(lastLogTerm==voteMessage.getLastLogTerm()&&voteMessage.getLastLogIndex()>=lastLogIndex))){

                            clusterState.setVotedFor(voteMessage.getCandidateId());
                            becomeFollower(voteMessage.getTerm(),true);
                            voteGranted=true;
                        }
                        voteRequest.response(new VoteResponse().setNodeId(clusterState.getId()).setVoteGranted(voteGranted).setTerm(clusterState.getCurrentTerm()));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });


    }
    private void handleVoteResponse(VoteResponse voteResponse){
        if (voteResponse.getTerm()>clusterState.getCurrentTerm()) {
            becomeFollower(voteResponse.getTerm(),true);
            return;
        }
        if (voteResponse.isVoteGranted()){
            logger.debug("get vote from node:{}",voteResponse.getNodeId());
            if (memberType==MemberType.PRE_CANDIDATES||memberType==MemberType.CANDIDATES) {
                if (memberType==MemberType.CANDIDATES&&voteResponse.isPreVote())
                    return;
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
        logger.debug("readIndex request");
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
        logger.debug("readIndex response id:{} readIndex:{} isLeader:{}",readIndexResponse.getId(),readIndexResponse.getReadIndex(),readIndexResponse.isLeader());
        String id = readIndexResponse.getId();
//        ReadState readState = readIndexMap.remove(id);
        ReadState readState = this.pendingReadState;
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


    private Future<Void> applyCommitIndex(){
        if (snapshotInstalling){
            return Future.succeededFuture();
        }
        Promise<Void> promise=Promise.promise();
        int lastApplied = clusterState.getLastApplied();
        int commitIndex = clusterState.getCommitIndex();
        logList.getLog(lastApplied+1,commitIndex+1)
                .onFailure(promise::fail)
                .onSuccess(list->{
                    if (!list.isEmpty()) {
                        list.forEach(stateService::applyLog);
                        LogEntry logEntry = list.get(list.size()-1);
                        clusterState.setLastApplied(logEntry.getIndex());
                    }
                    logCompact();
                    promise.complete();
                });
        return promise.future();
    }

    private void fireReadIndex(){
        int lastApplied = clusterState.getLastApplied();
        logger.debug("readState list size:{}",deque.size());
        while (!deque.isEmpty()){
            ReadState readState = deque.peekFirst();
            if (readState.readIndex>lastApplied){
                break;
            }
//            vertx.runOnContext(v->readState.promise.tryComplete(readState.readIndex));
            readState.promise.tryComplete(readState.readIndex);
            deque.poll();
        }
        /*vertx.executeBlocking(p->{

        },null);*/

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
        ThreadLocalRandom localRandom = ThreadLocalRandom.current();
        //FIXME debug
        int timeout = localRandom.nextInt(0, 150) + 1000;
        logger.debug("new election timeout: {}",timeout);
        this.timeoutStream=vertx.timerStream(timeout).handler(id->{
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
        //FIXME debug
        this.timeoutStream=vertx.timerStream(80).handler(id->{
           if (memberType==MemberType.LEADER){
               logger.debug("leader appendEntries term:{}",clusterState.getCurrentTerm());
               sendAppendEntries();
               setLeaderTimer();
           }else{
               logger.info("what happens");
           }
        });

    }

    private void becomeFollower(int term,boolean save){
        if (this.memberType==MemberType.LEADER){
            netCluster.nodes()
                    .forEach(cn->{
                        snapshot.readDone(cn.id());
                    });

        }
        this.memberType=MemberType.FOLLOWER;
        clusterState.setCurrentTerm(term);
        clusterState.setVotedFor(null);
        setTimer();
        this.counter =null;
        if (save)
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
        addLog(clusterState.getId(),0,0,null);
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
        if (preVoteRequest) {
            netCluster.nodes().forEach(cn->netCluster.request(cn.id(),voteRequest));
            return;
        }
        logList.getLastLog()
                .onFailure(t->logger.error("send vote request get last log failed",t))
                .onSuccess(lastLog->{
                    voteRequest.setTerm(clusterState.getCurrentTerm()).setCandidateId(clusterState.getId());
                    if (lastLog==null){
                        if (snapshot.meta().getIndex()==logList.getLastLogIndex()){
                            voteRequest.setLastLogIndex(snapshot.meta().getIndex());
                            voteRequest.setLastLogIndex(snapshot.meta().getTerm());
                        }else {
                            voteRequest.setLastLogIndex(0);
                            voteRequest.setLastLogTerm(0);
                        }
                    }else{
                        voteRequest.setLastLogIndex(lastLog.getIndex());
                        voteRequest.setLastLogTerm(lastLog.getTerm());
                    }
                    netCluster.nodes().forEach(cn->netCluster.request(cn.id(),voteRequest));
                });


    }

    private void sendAppendEntries(){
        netCluster.nodes().forEach(clusterNode -> {
            AppendEntriesMessage appendEntriesMessage = new AppendEntriesMessage()
                    .setTerm(clusterState.getCurrentTerm())
                    .setLeaderId(clusterState.getId())
                    .setLeaderCommit(clusterState.getCommitIndex());

            NodeState nodeState = clusterNode.state();
            logger.debug("node: {} log index range start:{} end: {}",clusterNode.id(),nodeState.getNextIndex(),clusterState.getLastIndex()+1);
            if (logList.getLastLogIndex()<=0){
                appendEntriesMessage.setPrevLogIndex(0);
                appendEntriesMessage.setPrevLogTerm(0);
                appendEntriesMessage.setEntries(Collections.emptyList());
                netCluster.request(clusterNode.id(),appendEntriesMessage);
            }else {
                int nextIndex = nodeState.getNextIndex();
                if (snapshot.meta().getIndex()!=0&&snapshot.meta().getIndex()>=nextIndex){
                    //sanpshot
                    snapshot.reader(clusterNode.id())
                            .onFailure(t->logger.error("get snapshot reader failed",t))
                            .onSuccess(reader->{
                                sendChunk(clusterNode,reader);
                            });
                    return;
                }
                int endIndex=(logList.getLastLogIndex() + 1)-nextIndex>100?nextIndex+100:(logList.getLastLogIndex() + 1);

                logger.debug("start:{} end:{}",nextIndex-1,endIndex);
                logList.getLog(nextIndex-1,endIndex)
                        .onFailure(t->logger.error("send append message to node:{} failed",t,clusterNode.id()))
                        .onSuccess(logs->{
                            if (logger.isDebugEnabled())
                                logger.debug("for node:{} get logs:{}",clusterNode.id(),logs);
                            try {
                                appendEntriesMessage.setEntries(logs);
                                if (nextIndex==1){
                                    appendEntriesMessage.setPrevLogIndex(0);
                                    appendEntriesMessage.setPrevLogTerm(0);
                                }else {
                                    if (nextIndex-1==snapshot.meta().getIndex()){
                                        appendEntriesMessage.setPrevLogIndex(snapshot.meta().getIndex());
                                        appendEntriesMessage.setPrevLogTerm(snapshot.meta().getTerm());
                                    }else {
                                        LogEntry logEntry = logs.remove(0);
                                        appendEntriesMessage.setPrevLogIndex(logEntry.getIndex());
                                        appendEntriesMessage.setPrevLogTerm(logEntry.getTerm());
                                    }
                                }
                                netCluster.request(clusterNode.id(),appendEntriesMessage);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });

            }

        });

    }

    private void sendChunk(ClusterNode clusterNode,SnapshotReader snapshotReader){

        SnapshotMeta meta = snapshot.meta();
        InstallSnapshotMessage installSnapshotMessage=new InstallSnapshotMessage()
                .setTerm(clusterState.getCurrentTerm())
                .setLeaderId(clusterState.getId())
                .setLastIncludeIndex(meta.getIndex())
                .setLastIncludeTerm(meta.getTerm())
                .setOffset(snapshotReader.getOffset());

        SnapshotReader.SnapshotChunk currentChunk = snapshotReader.getCurrentChunk();
        if (currentChunk!=null&&(snapshotReader.isEnd()||currentChunk.getOffset()==snapshotReader.getOffset())){
            installSnapshotMessage
                    .setDone(snapshotReader.isEnd())
                    .setBuffer(currentChunk.getBuffer());
            netCluster.request(clusterNode.id(),installSnapshotMessage);
            return;
        }
        snapshotReader.nextChunk()
                .onFailure(tt->logger.error("read offset:{} chunk failed",tt,snapshotReader.getOffset()))
                .onSuccess(buffer->{
                    installSnapshotMessage
                            .setDone(snapshotReader.isEnd())
                            .setBuffer(buffer);
                    netCluster.request(clusterNode.id(),installSnapshotMessage);
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

            if (pendingReadState==null){
                ReadState readState = new ReadState(id);
                readState.setTimer();
                this.pendingReadState=readState;
                netCluster.requestReadIndex(leaderId,id);
                return readState.promise.future();
            }

            return pendingReadState.promise.future();
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

    public void addLog(String nodeId,int proposalId,int lowestProposalId,Buffer buffer){
        logList.addLog(nodeId,clusterState.getCurrentTerm(),proposalId,lowestProposalId,buffer);
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
            //FIXME debug
            this.timeoutStream=vertx.timerStream(3000);
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
