package com.stormpx.kit;

import io.netty.handler.codec.EncoderException;
import io.netty.util.CharsetUtil;

public class MqttCodecUtil {


    public static byte[] encodeUtf8String(String str){
        if (str==null)
            return new byte[0];
        return str.getBytes(CharsetUtil.UTF_8);
    }

    public static byte[] encodeVariableLength(int length){
        int v = getVariableLength(length);
        byte[] bytes=new byte[v];
        int i=0;
        do {
            byte encodedByte = (byte) (length % 128);
            length = length / 128;
            if (length > 0) {
                encodedByte |= 128;
            }
            bytes[i++]= encodedByte;

        }while (length > 0);

        return bytes;
    }

    public static int getVariableLength(int length){
        int i=0;
        do {
            length/=128;
            i+=1;
        }while (length>0);
        return i;
    }


}
