package com.stormpx.broker;

import com.stormpx.kit.StringPair;
import com.stormpx.kit.TopicUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RetainMap {
    //k topic v id
    private Map<String,String> retainMap;


    public RetainMap(Map<String, String> retainMap) {
        this.retainMap = retainMap;
    }


    /**
     * k topic v id
     * @param topics
     * @return
     */
    public List<StringPair> match(List<String> topics){
        return retainMap.entrySet()
                .stream()
                .filter(e->topics.stream().anyMatch(o-> TopicUtil.matches(o,e.getKey())))
                .filter(e->e.getValue()!=null)
                .map(e->new StringPair(e.getKey(),e.getValue()))
                .collect(Collectors.toList());
    }

}
