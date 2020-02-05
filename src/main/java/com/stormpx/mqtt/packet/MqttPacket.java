package com.stormpx.mqtt.packet;

import com.stormpx.mqtt.FixedHeader;

public interface MqttPacket {

    FixedHeader fixedHeader();

    default boolean invalid(){
        return cause()!=null;
    }

    default Throwable cause(){
        return null;
    }

}
