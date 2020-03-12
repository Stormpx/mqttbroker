package com.stormpx.kit;

import io.vertx.core.buffer.Buffer;

import java.nio.charset.StandardCharsets;

public class MsgPackUtil {

    public static Buffer encodeInt(Number number){
        long longValue = number.longValue();
        if (longValue<128){
            return Buffer.buffer(2).appendByte((byte) 0xd0).appendByte((byte) longValue);
        }else if (longValue<Short.MAX_VALUE+1){
            return Buffer.buffer(3).appendByte((byte) 0xd1).appendShort((short) longValue);
        }else if (longValue< ((long)Integer.MAX_VALUE ) + 1){
            return Buffer.buffer(5).appendByte((byte) 0xd2).appendInt((int) longValue);
        }else{
            return Buffer.buffer(9).appendByte((byte) 0xd3).appendLong(longValue);
        }
    }

    public static Buffer encodeStr(String str){
        byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
        if (strBytes.length<32){
            return Buffer.buffer(1+strBytes.length)
                    .appendByte((byte) (0xa0 |strBytes.length))
                    .appendBytes(strBytes);
        }else if (strBytes.length<256){
            return Buffer.buffer(2+strBytes.length)
                    .appendByte((byte) 0xd9)
                    .appendUnsignedByte((short) strBytes.length)
                    .appendBytes(strBytes);

        }else if (strBytes.length<65536){
            return Buffer.buffer(3+strBytes.length)
                    .appendByte((byte) 0xda)
                    .appendUnsignedShort(strBytes.length)
                    .appendBytes(strBytes);
        }else {
            return Buffer.buffer(5+strBytes.length)
                    .appendByte((byte) 0xdb)
                    .appendInt(strBytes.length)
                    .appendBytes(strBytes);
        }
    }

    public static Buffer encodeArrayLength(int length){
        if (length< 16){
            return Buffer.buffer().appendByte((byte) (0x90|length));
        }else if (length<65536){
            return Buffer.buffer(3)
                    .appendByte((byte) 0xdc)
                    .appendUnsignedShort(length);
        }else {
            return Buffer.buffer(5)
                    .appendByte((byte) 0xdd)
                    .appendInt(length);
        }
    }

}
