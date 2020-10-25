package com.stormpx.cluster.mqtt;


import java.util.Map;
import java.util.Set;

public class RetainMatchResult {

    private Map<String, Set<String>> matchMap;

    public Map<String, Set<String>> getMatchMap() {
        return matchMap;
    }

    public RetainMatchResult setMatchMap(Map<String, Set<String>> matchMap) {
        this.matchMap = matchMap;
        return this;
    }

}
