package com.stormpx.kit.value;

public interface Values5<O1,O2,O3,O4,O5> {
    O1 getOne();
    O2 getTwo();
    O3 getThree();
    O4 getFour();
    O5 getFive();

    static <O1,O2,O3,O4,O5> Values5<O1,O2,O3,O4,O5> values(O1 o1, O2 o2, O3 o3,O4 o4,O5 o5){
        return new Values5Impl<>(o1,o2,o3,o4,o5);
    }
}
