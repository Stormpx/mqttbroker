package com.stormpx.cluster.net;

import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.message.VoteMessage;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;

public class SocketHandler implements Handler<Buffer> {
    private MessageType messageType;
    private Integer requestId;
    private CompositeByteBuf buf;
    private Integer expectedLength;
    private Handler<RpcMessage> handler;

    public SocketHandler() {
        this.buf= Unpooled.compositeBuffer();
    }


    public SocketHandler messageHandler(Handler<RpcMessage> handler){
        this.handler=handler;
        return this;
    }

    @Override
    public void handle(Buffer buffer) {
        buf.addComponent(true,buffer.getByteBuf());
        decode();
        buf.discardReadComponents();
    }

    private void decode(){
        if (messageType==null){
            if (!buf.isReadable())
                return;
            messageType=MessageType.valueOf(buf.readByte());
        }
        if (messageType==MessageType.REQUEST&&requestId==null){
            if (buf.readableBytes()<4) {
                return;
            }
            requestId=buf.readInt();
        }
        if (expectedLength==null){
            if (buf.readableBytes()<4) {
                return;
            }
            expectedLength = buf.readInt();
        }
        if (buf.readableBytes()>=expectedLength){
            Buffer payload = Buffer.buffer(buf.readSlice(expectedLength));
            Handler<RpcMessage> handler = this.handler;
            if (handler!=null) {
                try {
                    handler.handle(new RpcMessage().setMessageType(messageType).setRequestId(requestId==null?0:requestId).setBuffer(payload));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            resetState();
            decode();
        }

    }
    private void resetState(){
        expectedLength=null;
        messageType=null;
        requestId=null;
    }

}
