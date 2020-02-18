package com.stormpx.cluster;

import java.util.HashSet;
import java.util.Set;

public class Counter {
    private int target;
    private Set<String> set;


    public Counter(int nodeCount) {
        this.target = nodeCount/2+1;
        this.set=new HashSet<>();
    }


    public void add(String id){
        this.set.add(id);
    }

    public boolean isMajority(){
        return set.size()>=target;
    }

}
