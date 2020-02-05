package com.stormpx.kit.value;


public class Values1Impl<O1> implements Values1<O1> {

    private O1 o1;

    Values1Impl(O1 o1) {
        this.o1 = o1;
    }

    @Override
    public O1 getOne() {
        return o1;
    }

    @Override
    public <O2> Values2<O1, O2> toValues2(O2 t) {
        return Values2.values(o1,t);
    }


}
