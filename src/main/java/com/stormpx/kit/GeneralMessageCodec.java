package com.stormpx.kit;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

public class GeneralMessageCodec<T> implements MessageCodec<T,T> {

    private String name;
    private Function<T,Buffer> encodeFunction;
    private BiFunction<Integer,Buffer,T> decodeFunction;

    public GeneralMessageCodec() {

        Type type = getClass().getGenericSuperclass();
        if (!(type instanceof ParameterizedType)){
            throw new RuntimeException("GeneralMessageCodec must a anonymous class");
        }
        ParameterizedType parameterizedType = ParameterizedType.class.cast(type);
        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
        Class<?> actualTypeArgument = (Class<?>) actualTypeArguments[0];
        this.name= actualTypeArgument.getSimpleName();
    }


    @Override
    public void encodeToWire(Buffer buffer, T t) {
        if (encodeFunction!=null){
            buffer.appendBuffer(encodeFunction.apply(t));
        }
    }

    @Override
    public T decodeFromWire(int pos, Buffer buffer) {
        if (decodeFunction!=null){
            return decodeFunction.apply(pos,buffer);
        }
        return null;
    }

    @Override
    public T transform(T t) {
        return t;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }

    public GeneralMessageCodec<T> encode(Function<T, Buffer> encodeFunction) {
        this.encodeFunction = encodeFunction;
        return this;
    }

    public GeneralMessageCodec<T> decode(BiFunction<Integer, Buffer, T> decodeFunction) {
        this.decodeFunction = decodeFunction;
        return this;
    }

}
