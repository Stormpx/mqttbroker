package com.stormpx.cluster.message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ActionLog  {
    private String action;
    private List<String> args;


    public static ActionLog subscribe(String nodeId,List<String> topic){
        ActionLog actionLog = new ActionLog().setAction(Action.SUBSCRIBE.value);
        List<String> args=new ArrayList<>(1+topic.size());
        args.add(nodeId);
        args.addAll(topic);
        actionLog.setArgs(args);
        return actionLog;
    }

    public static ActionLog unSubscribe(String nodeId,List<String> topic){
        ActionLog actionLog = new ActionLog().setAction(Action.UNSUBSCRIBE.value);
        List<String> args=new ArrayList<>(1+topic.size());
        args.add(nodeId);
        args.addAll(topic);
        actionLog.setArgs(args);
        return actionLog;
    }
    public static ActionLog saveMessage(String nodeId,String id,boolean retain,String topic,int payloadLength){
        ActionLog actionLog = new ActionLog().setAction(Action.SAVEMESSAGE.value);
        List<String> args = new ArrayList<>(Arrays.asList(nodeId, id));
        if (retain){
            args.add("y");
            args.add(topic);
            args.add(String.valueOf(payloadLength));
        }else{
            args.add("n");
        }
        actionLog.setArgs(args);
        return actionLog;
    }

    public static ActionLog delMessage(String nodeId,String id){
        ActionLog actionLog = new ActionLog().setAction(Action.DELMESSAGE.value);
        actionLog.setArgs(Arrays.asList(nodeId, id));
        return actionLog;
    }

    public static ActionLog saveSession(String nodeId,String clientId,boolean reset){
        ActionLog actionLog = new ActionLog().setAction(Action.SAVESESSION.value);
        actionLog.setArgs(Arrays.asList(nodeId,clientId,reset?"y":"n"));
        return actionLog;
    }


    public static ActionLog delSession(String nodeId,String clientId){
        ActionLog actionLog = new ActionLog().setAction(Action.DELSESSION.value);
        return actionLog.setArgs(Arrays.asList(nodeId,clientId));
    }


    public String getAction() {
        return action;
    }

    public ActionLog setAction(String action) {
        this.action = action;
        return this;
    }

    public List<String> getArgs() {
        return args;
    }

    public ActionLog setArgs(List<String> args) {
        this.args = args;
        return this;
    }

    @Override
    public String toString() {
        return "ActionLog{" + "action='" + action + '\'' + ", args=" + args + '}';
    }

    public enum Action{
        SUBSCRIBE("subscribe"),
        UNSUBSCRIBE("unSubscribe"),
        SAVEMESSAGE("saveMessage"),
        DELMESSAGE("delMessage"),
        SAVESESSION("saveSession"),
        DELSESSION("delSession")
        ;

        private String value;
        Action(String value) {
            this.value=value;
        }


        public static Action of(String action){
            for (Action value : values()) {
                if (value.value.equals(action))
                    return value;
            }

            return null;
        }

    }
}
