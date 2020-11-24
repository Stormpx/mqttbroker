package com.stormpx.dispatcher;

public interface MessageHook {


    void onSaveMessage(DispatcherMessage message);


    void onDelMessage(String id);
}
