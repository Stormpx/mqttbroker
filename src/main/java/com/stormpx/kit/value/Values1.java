package com.stormpx.kit.value;


public interface Values1<O1> {


    O1 getOne();

    <O2> Values2<O1, O2> toValues2(O2 t);


    static <O1> Values1<O1> values(O1 o1){
        return new Values1Impl<>(o1);
    }
}
