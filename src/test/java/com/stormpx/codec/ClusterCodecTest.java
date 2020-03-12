package com.stormpx.codec;

import com.stormpx.cluster.LogEntry;
import com.stormpx.cluster.message.*;
import com.stormpx.cluster.mqtt.MqttMetaData;
import com.stormpx.cluster.net.SocketHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

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

    @Test
    public void installSnapshotMessageTest(){
        String s=".spleh A+lrtC/dmC .thgis fo tuo si ti semitemos ,etihw si txet nehw sa drah kooL .tseretni wohs dluohs uoy ecalp a si ,dessecorp si xat hctuD erehw esac ehT .sedih tseuq fo txen eht erehw si ,deificeps era segaugnal cificeps-niamod tcudorp ehT";

        System.out.println(new StringBuilder(s).reverse().toString());

        InstallSnapshotMessage installSnapshotMessage = new InstallSnapshotMessage().setTerm(1)
                .setLeaderId("weqwe")
                .setLastIncludeTerm(2).setLastIncludeTerm(1).setOffset(23).setDone(true).setBuffer(Buffer.buffer("awewqew"));
        Buffer encode = installSnapshotMessage.encode();
        InstallSnapshotMessage message = InstallSnapshotMessage.decode(encode);

        Assertions.assertEquals(message.getLeaderId(),installSnapshotMessage.getLeaderId());
        Assertions.assertEquals(message.getTerm(),installSnapshotMessage.getTerm());
        Assertions.assertEquals(message.getLastIncludeIndex(),installSnapshotMessage.getLastIncludeIndex());
        Assertions.assertEquals(message.getLastIncludeTerm(),installSnapshotMessage.getLastIncludeTerm());
        Assertions.assertEquals(message.getOffset(),installSnapshotMessage.getOffset());
        Assertions.assertEquals(message.isDone(),installSnapshotMessage.isDone());
        Assertions.assertEquals(message.getBuffer(),installSnapshotMessage.getBuffer());


    }

    @Test
    public void metaDataTest(){
        MqttMetaData mqttMetaData = new MqttMetaData();
        mqttMetaData.addSubscription("qwe",Arrays.asList("2312","testa","31532","testga","aegae","afweafew"));
        mqttMetaData.addSubscription("faefw",Arrays.asList("qewq","gesgs","gesa","gesa","dwa","geas","gsgse","grdshgd"));

        for (int i = 0; i < 3000; i++) {
            mqttMetaData.setExecute("node1",i);
        }
        for (int i = 0; i < 50000; i++) {
            mqttMetaData.setExecute("node2",i);
        }
        mqttMetaData.putRetain("qwe","fawf");
        mqttMetaData.putRetain("fafw","asdw");
        mqttMetaData.putRetain("gesag","ewa");
        mqttMetaData.putRetain("sage","weq");
        mqttMetaData.putRetain("gesag","hgrds");

        mqttMetaData.saveSession("node1","client1");
        mqttMetaData.saveSession("node2","client1");
        mqttMetaData.saveSession("node3","client1");
        mqttMetaData.saveSession("node4","client1");
        mqttMetaData.saveSession("node5","client1");

        mqttMetaData.saveSession("node1","client0");

        mqttMetaData.saveMessage("node1","message1");
        mqttMetaData.saveMessage("node2","message1");
        mqttMetaData.saveMessage("node3","message1");
        mqttMetaData.saveMessage("node4","message1");
        mqttMetaData.saveMessage("node5","message1");

        mqttMetaData.saveMessage("node1","message0");

        Buffer buffer=Buffer.buffer().appendBuffer(mqttMetaData.encodeSubscribe())
                .appendBuffer(mqttMetaData.encodeRequestId())
                .appendBuffer(mqttMetaData.encodeIdIndex())
                .appendBuffer(mqttMetaData.encodeSessionIndex())
                .appendBuffer(mqttMetaData.encodeRetain());

        MqttMetaData metaData = new MqttMetaData();
        metaData.decode(buffer);

        Assertions.assertEquals(mqttMetaData,metaData);


    }

}