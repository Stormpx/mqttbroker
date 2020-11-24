package com.stormpx.kit.value;

public class Values4Impl<O1,O2,O3,O4> implements Values4<O1,O2,O3,O4> {

    private O1 o1;
    private O2 o2;
    private O3 o3;
    private O4 o4;

    public Values4Impl(O1 o1, O2 o2, O3 o3,O4 o4) {
        this.o1 = o1;
        this.o2 = o2;
        this.o3 = o3;
        this.o4=o4;
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

    @Override
    public O4 getFour() {
        return o4;
    }

    @Override
    public <O5> Values5<O1, O2, O3, O4, O5> toValues5(O5 o5) {
        return Values5.values(o1,o2,o3,o4,o5);
    }
}
