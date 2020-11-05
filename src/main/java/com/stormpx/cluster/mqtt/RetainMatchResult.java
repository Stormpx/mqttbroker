package com.stormpx.cluster.mqtt;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RetainMatchResult {

    private Map<String, MatchMessageIndex> matchMap=new HashMap<>();


    public void addMatch(String topic,String mid,Set<String> nodeSet){
        matchMap.put(topic,new MatchMessageIndex(mid,nodeSet));
    }


    public Map<String, MatchMessageIndex> getMatchMap() {
        return matchMap;
    }



    public class MatchMessageIndex {

        private String id;
        private Set<String> nodeIds;

        public MatchMessageIndex(String id, Set<String> nodeIds) {
            this.id = id;
            this.nodeIds = nodeIds;
        }

        public String getId() {
            return id;
        }

        public Set<String> getNodeIds() {
            return nodeIds;
        }
    }
}
