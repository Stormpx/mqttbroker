package com.stormpx.auth;

import com.stormpx.kit.StringPair;

import java.util.List;

public class AuthResult<T> {
    private T t;
    private List<StringPair> pairList;

    private AuthResult(T t) {
        this.t = t;
    }

    public static <R> AuthResult<R> create(R r){
        return new AuthResult<>(r);
    }


    public T getObject() {
        return t;
    }

    public AuthResult<T> setPairList(List<StringPair> pairList) {
        this.pairList = pairList;
        return this;
    }

    public List<StringPair> getPairList() {
        return pairList;
    }
}
