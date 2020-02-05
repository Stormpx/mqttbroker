package com.stormpx;

import com.stormpx.kit.TopicFilter;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(VertxExtension.class)
class TopicFilterTest {




    @Test
    void subscribe() {
        TopicFilter topicFilter = new TopicFilter();
        Assertions.assertFalse(topicFilter.subscribe("","a0", MqttQoS.AT_MOST_ONCE,false,false,0));
        Assertions.assertTrue(topicFilter.subscribe("/","a0", MqttQoS.EXACTLY_ONCE,false,false,0));
        Assertions.assertTrue(topicFilter.subscribe("#","a1", MqttQoS.AT_LEAST_ONCE,false,false,0));
        Assertions.assertTrue(topicFilter.subscribe("test","a1", MqttQoS.AT_MOST_ONCE,false,false,0));
        Assertions.assertTrue(topicFilter.subscribe("/test","a2", MqttQoS.AT_MOST_ONCE,false,false,0));
        Assertions.assertTrue(topicFilter.subscribe("$SYS","a3", MqttQoS.AT_MOST_ONCE,false,false,0));

    }



    @Test
    void matches() {
        TopicFilter topicFilter=new TopicFilter();

        topicFilter.subscribe("/test","a",MqttQoS.AT_LEAST_ONCE,false,false,0);
        topicFilter.subscribe("/test/foo","a",MqttQoS.EXACTLY_ONCE,false,false,0);
        topicFilter.subscribe("/adwada/dwafsg/asf/desa/dsa/d/awd/aw/fas/fv/x/fas/fd/sag/eaw/gsad/f/asdf/asd/wa/d/as/d/as/fs/g/ds/gds/c/dsa/w/d/sad/sa/dsa/c/sc/s/av/dsf/es/af/saef/esdwadfawfesfsdgafhsiuoehdfioashdioawhdioahdskdhiowahdfioawdwa/fwa/fdw/af/saf/sa/dfs/f/reag/e/asd/f/ewa/dfsd/f/sae/fqa/wef/aw/sf/sd/fes/afd/sf/s/fes/f/esf/es/fes/af/es/fs/f/se/f/s/fs/af/es","a",MqttQoS.EXACTLY_ONCE,false,false,0);
        topicFilter.subscribe("/test/","b",MqttQoS.AT_LEAST_ONCE,false,false,0);
        topicFilter.subscribe("/test/foo","b",MqttQoS.AT_LEAST_ONCE,false,false,0);
        topicFilter.subscribe("/test/bar","b",MqttQoS.AT_LEAST_ONCE,false,false,0);
        assertEquals(1, topicFilter.matches("/test").size());
        assertEquals(1, topicFilter.matches("/test/").size());
        assertEquals(2, topicFilter.matches("/test/foo").size());
        assertEquals(1, topicFilter.matches("/test/bar").size());
        assertEquals(1,topicFilter.matches("/adwada/dwafsg/asf/desa/dsa/d/awd/aw/fas/fv/x/fas/fd/sag/eaw/gsad/f/asdf/asd/wa/d/as/d/as/fs/g/ds/gds/c/dsa/w/d/sad/sa/dsa/c/sc/s/av/dsf/es/af/saef/esdwadfawfesfsdgafhsiuoehdfioashdioawhdioahdskdhiowahdfioawdwa/fwa/fdw/af/saf/sa/dfs/f/reag/e/asd/f/ewa/dfsd/f/sae/fqa/wef/aw/sf/sd/fes/afd/sf/s/fes/f/esf/es/fes/af/es/fs/f/se/f/s/fs/af/es").size());
        assertEquals(0,topicFilter.matches("/tt/ta").size());

        topicFilter.subscribe("/test/qos0","c1",MqttQoS.AT_MOST_ONCE,false,false,0);
        topicFilter.subscribe("/test/qos1","c1",MqttQoS.AT_LEAST_ONCE,false,false,0);
        topicFilter.subscribe("/test/qos2","c1",MqttQoS.EXACTLY_ONCE,false,false,0);
        assertEquals(1, topicFilter.matches("/test/qos0").size());
        assertEquals(1, topicFilter.matches("/test/qos1").size());
        assertEquals(1, topicFilter.matches("/test/qos2").size());


    }




    @Test
    void multiLevelWildcard(){
        TopicFilter topicFilter = new TopicFilter();

        topicFilter.subscribe("sport/tennis/player1/#","client1",MqttQoS.EXACTLY_ONCE,false,false,0);

        assertEquals(1,topicFilter.matches("sport/tennis/player1").size());
        assertEquals(1,topicFilter.matches("sport/tennis/player1/ranking").size());
        assertEquals(1,topicFilter.matches("sport/tennis/player1/score/wimbledon").size());
        assertEquals(1,topicFilter.matches("sport/tennis/player1//qwb").size());
        assertEquals(0,topicFilter.matches("sport/tenni/player1//qwb").size());


        topicFilter.subscribe("sport/#","client2",MqttQoS.EXACTLY_ONCE,false,false,0);
        assertEquals(1,topicFilter.matches("sport").size());
        assertEquals(1,topicFilter.matches("sport/abc").size());
        assertEquals(1,topicFilter.matches("sport/abc/wqea").size());

        topicFilter.subscribe("foo/bar/#","client3",MqttQoS.EXACTLY_ONCE,false,false,0);
        topicFilter.subscribe("foo/bar#","client4",MqttQoS.EXACTLY_ONCE,false,false,0);
        topicFilter.subscribe("foo/bar/#/test","client5",MqttQoS.EXACTLY_ONCE,false,false,0);

        assertEquals(1,topicFilter.matches("foo/bar/test").size());

        topicFilter.subscribe("#","client6",MqttQoS.EXACTLY_ONCE,false,false,0);
        assertEquals(1,topicFilter.matches("test/test/test").size());
        assertEquals(1,topicFilter.matches("////").size());



    }


    @Test
    void singleLevelWildcard(){
        TopicFilter topicFilter = new TopicFilter();

        topicFilter.subscribe("sport/tennis/+","client1",MqttQoS.EXACTLY_ONCE,false,false,0);
        assertEquals(1,topicFilter.matches("sport/tennis/player1").size());
        assertEquals(1,topicFilter.matches("sport/tennis/player2").size());
        assertEquals(0,topicFilter.matches("sport/tennis/player1/ranking").size());

        topicFilter.subscribe("sport/+","client2",MqttQoS.EXACTLY_ONCE,false,false,0);
        topicFilter.subscribe("sprot+","client5",MqttQoS.EXACTLY_ONCE,false,false,0);
        topicFilter.subscribe("sport/+/player1","client6",MqttQoS.EXACTLY_ONCE,false,false,0);

        assertEquals(0,topicFilter.matches("sport").size());
        assertEquals(1,topicFilter.matches("sport/").size());
        assertEquals(1,topicFilter.matches("sport/football/player1").size());


        topicFilter.subscribe("+","client3",MqttQoS.EXACTLY_ONCE,false,false,0);
        topicFilter.subscribe("+/tennis/#","client4",MqttQoS.EXACTLY_ONCE,false,false,0);

        assertEquals(1,topicFilter.matches("foo/tennis").size());
        assertEquals(1,topicFilter.matches("/tennis").size());
        assertEquals(1,topicFilter.matches("bar/tennis/asbc/awe").size());
        assertEquals(0,topicFilter.matches("/bar/tennis/asbc/awe").size());



        topicFilter.subscribe("+/+","client7",MqttQoS.EXACTLY_ONCE,false,false,0);
        topicFilter.subscribe("/+","client8",MqttQoS.EXACTLY_ONCE,false,false,0);

        assertEquals(2,topicFilter.matches("/finance").size());
        assertEquals(1 ,topicFilter.matches("finance").size());

    }
}