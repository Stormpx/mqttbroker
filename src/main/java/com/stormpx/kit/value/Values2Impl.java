package com.stormpx.kit.value;


public class Values2Impl<O1,O2> implements Values2<O1,O2> {
    private O1 o1;
    private O2 o2;

    public Values2Impl(O1 o1, O2 o2) {
        this.o1 = o1;
        this.o2 = o2;
    }

    @Override
    public O2 getTwo() {
        return o2;
    }

    @Override
    public <O3> Values3<O1, O2, O3> toValues3(O3 t) {
        return Values3.values(o1,o2,t);
    }

    @Override
    public O1 getOne() {
        return o1;
    }

}
