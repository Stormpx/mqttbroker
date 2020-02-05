package com.stormpx.kit.value;


public interface Values2<O1,O2>  {

    O1 getOne();
    O2 getTwo();

    <O3> Values3<O1, O2,O3> toValues3(O3 t);


    static <O1,O2> Values2<O1,O2> values(O1 o1, O2 o2){
        return new Values2Impl<>(o1,o2);
    }

}
