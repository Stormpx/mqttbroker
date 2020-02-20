package com.stormpx.cluster.net;

import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.RpcMessage;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

public class SocketHandler implements Handler<Buffer> {
    private MessageType messageType;
    private Integer nodeIdLength;
    private String nodeId;
    private Integer requestId;
    private CompositeByteBuf buf;
    private Integer payloadLength;
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
        if (nodeIdLength==null){
            if (buf.readableBytes()<2)
                return;
            nodeIdLength=buf.readUnsignedShort();
        }
        if (nodeId==null){
            if (buf.readableBytes()<nodeIdLength){
                return;
            }
            nodeId=buf.readSlice(nodeIdLength).toString(CharsetUtil.UTF_8);
        }
        if (messageType==MessageType.REQUEST&&requestId==null){
            if (buf.readableBytes()<4) {
                return;
            }
            requestId=buf.readInt();
        }
        if (payloadLength ==null){
            if (buf.readableBytes()<4) {
                return;
            }
            payloadLength = buf.readInt();
        }
        if (buf.readableBytes()>= payloadLength){
            Buffer payload = Buffer.buffer(buf.readSlice(payloadLength));
            Handler<RpcMessage> handler = this.handler;
            if (handler!=null) {
                try {
                    handler.handle(new RpcMessage().setMessageType(messageType).setFromId(nodeId).setRequestId(requestId).setRequestId(requestId==null?0:requestId).setBuffer(payload));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            resetState();
            decode();
        }

    }
    private void resetState(){
        payloadLength =null;
        nodeIdLength=null;
        nodeId=null;
        messageType=null;
        requestId=null;
    }

}
