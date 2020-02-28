package com.stormpx.codec;

import com.stormpx.cluster.LogEntry;
import com.stormpx.cluster.message.*;
import com.stormpx.cluster.net.SocketHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class ClusterCodecTest {

    @Test
    public void rpcMessageTest(){
        RpcMessage rpcMessage = new RpcMessage(MessageType.VOTEREQUEST, "123", "1242", 0, Buffer.buffer("test"));
        Buffer encode = rpcMessage.encode();
        new SocketHandler().messageHandler(rpc->{
            Assertions.assertEquals(rpc.getMessageType(),MessageType.VOTEREQUEST);
            Assertions.assertEquals(rpc.getTargetId(),"123");
            Assertions.assertEquals(rpc.getFromId(),"1242");
            Assertions.assertEquals(rpc.getRequestId(),0);
            Assertions.assertEquals(rpc.getBuffer().toString("utf-8"),"test");
        }).handle(encode);
    }

    @Test
    public void proMessageTest(){
        ActionLog actionLog = ActionLog.saveMessage("123", "wqe", false, "213", 0);
        ProMessage proMessage = new ProMessage("addLog", JsonObject.mapFrom(actionLog));
        Buffer encode = proMessage.encode();
        ProMessage message = ProMessage.decode(encode);

        Assertions.assertEquals(message.getRes(),proMessage.getRes());
        Assertions.assertEquals(message.getBody(),JsonObject.mapFrom(actionLog));

    }

    @Test
    public void logEntryTest(){
        JsonObject json = JsonObject.mapFrom(ActionLog.saveMessage("123", "wqe", false, "213", 0));
        LogEntry logEntry = new LogEntry().setTerm(1).setIndex(1).setNodeId("node1").setRequestId(1).setPayload(json.toBuffer());
        Buffer buffer = logEntry.encode();

        LogEntry entry = LogEntry.decode(0, buffer);

        Assertions.assertEquals(entry.getIndex(),logEntry.getIndex());
        Assertions.assertEquals(entry.getTerm(),logEntry.getTerm());
        Assertions.assertEquals(entry.getRequestId(),logEntry.getRequestId());
        Assertions.assertEquals(entry.getNodeId(),logEntry.getNodeId());
        Assertions.assertEquals(entry.getPayload().toJsonObject(),logEntry.getPayload().toJsonObject());

    }

    @Test
    public void appendEntriesMessageTest(){
        JsonObject json = JsonObject.mapFrom(ActionLog.saveMessage("123", "wqe", false, "213", 0));
        LogEntry logEntry1 = new LogEntry().setTerm(1).setIndex(1).setNodeId("node1").setRequestId(1).setPayload(json.toBuffer());
        LogEntry logEntry2 = new LogEntry().setTerm(2).setIndex(2).setNodeId("node2").setRequestId(2).setPayload(json.toBuffer());
        AppendEntriesMessage appendEntriesMessage = new AppendEntriesMessage().setTerm(1).setPrevLogIndex(1).setPrevLogTerm(1).setLeaderId("node1")
                .setLeaderCommit(1)
                .setEntries(Arrays.asList(logEntry1, logEntry2));

        Buffer encode = appendEntriesMessage.encode();

        AppendEntriesMessage appendEntriesMessage1 = AppendEntriesMessage.decode(encode);

        Assertions.assertEquals(appendEntriesMessage1.getTerm(),appendEntriesMessage.getTerm());
        Assertions.assertEquals(appendEntriesMessage1.getPrevLogIndex(),appendEntriesMessage.getPrevLogIndex());
        Assertions.assertEquals(appendEntriesMessage1.getPrevLogTerm(),appendEntriesMessage.getPrevLogTerm());
        Assertions.assertEquals(appendEntriesMessage1.getLeaderId(),appendEntriesMessage.getLeaderId());
        Assertions.assertEquals(appendEntriesMessage1.getLeaderCommit(),appendEntriesMessage.getLeaderCommit());
        Assertions.assertEquals(appendEntriesMessage1.getEntries(),appendEntriesMessage.getEntries());

    }
}
