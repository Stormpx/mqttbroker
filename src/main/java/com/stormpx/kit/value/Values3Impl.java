package com.stormpx.kit.value;


public class Values3Impl<O1,O2,O3> implements Values3<O1,O2,O3> {
    private O1 o1;
    private O2 o2;
    private O3 o3;

    public Values3Impl(O1 o1, O2 o2, O3 o3) {
        this.o1 = o1;
        this.o2 = o2;
        this.o3 = o3;
    }

    @Override
    public O1 getOne() {
        return o1;
    }

    @Override
    public O2 getTwo() {
        return o2;
    }

    @Override
    public O3 getThree() {
        return o3;
    }
}
