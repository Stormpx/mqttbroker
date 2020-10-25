package com.stormpx.kit.value;

public interface Values4<O1,O2,O3,O4> {


    O1 getOne();
    O2 getTwo();
    O3 getThree();
    O4 getFour();

    <O5> Values5<O1, O2,O3,O4,O5> toValues5(O5 o5);

    static <O1,O2,O3,O4> Values4<O1,O2,O3,O4> values(O1 o1, O2 o2, O3 o3,O4 o4){
        return new Values4Impl<>(o1,o2,o3,o4);
    }
}
