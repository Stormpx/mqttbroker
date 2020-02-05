package com.stormpx;

public class Constants {



    public final static String SNI ="sni";
    public final static String TCP_NO_DELAY ="tcp_no_delay";

    public final static String TCP="tcp";
    public final static String WS="ws";
    public final static String ENABLE ="enable";
    public final static String SSL ="ssl";
    public final static String HOST ="host";
    public final static String PORT ="port";
    public final static String PATH ="path";
    public static final String KEY_CERT = "key_cert";
    public static final String KEY_PATH = "key_path";
    public static final String CERT_PATH = "cert_path";


    public final static String AUTH ="auth";

    public final static String MQTT="mqtt";
    public final static String MQTT_MAX_MESSAGE_EXPIRY_INTERVAL="mqtt_max_message_expiry_interval";
    public final static String MQTT_MAX_SESSION_EXPIRY_INTERVAL="mqtt_max_session_expiry_interval";

    public final static String MQTT_MAXIMUM_QOS="maximum_qos";
    public final static String MQTT_MAXIMUM_PACKET_SIZE="maximum_packet_size";
    public final static String MQTT_RECEIVE_MAXIMUM="receive_maximum";
    public final static String MQTT_TOPIC_ALIAS_MAXIMUM="topic_alias_maximum";
    public final static String MQTT_RETAIN_AVAILABLE="retain_available";
    public final static String MQTT_SERVER_KEEP_ALIVE="server_keep_alive";
    public final static String MQTT_WILDCARD_SUBSCRIPTION_AVAILABLE="wildcard_subscription_available";
    public final static String MQTT_SUBSCRIPTION_IDENTIFIER_AVAILABLE="subscription_identifier_available";
    public final static String MQTT_SHARED_SUBSCRIPTION_AVAILABLE="shared_subscription_available";

    public final static String PERSISTENCE ="persistence";
    public final static String SAVE_ENABLE ="save_enable";
    public final static String SAVE_INTERVAL ="save_interval";
    public final static String SAVE_DIR ="save_dir";

    public static String toAddress(String topic){
        return "topic-"+topic;
    }

}
