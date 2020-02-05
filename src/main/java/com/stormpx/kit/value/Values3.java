package com.stormpx.kit.value;


public interface Values3<O1,O2,O3>  {

    O1 getOne();
    O2 getTwo();
    O3 getThree();

    static <O1,O2,O3> Values3<O1,O2,O3> values(O1 o1, O2 o2, O3 o3){
        return new Values3Impl<>(o1,o2,o3);
    }


}
