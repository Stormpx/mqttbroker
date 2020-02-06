package com.stormpx.auth;

import com.stormpx.message.MqttAuth;
import com.stormpx.kit.J;
import com.stormpx.kit.StringPair;
import com.stormpx.kit.value.Values3;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.mqtt.ReasonCode;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigAuthenticator implements Authenticator{

    private Map<String, User> userMap=new HashMap<>();
    private Map<String,Permissions> permissionMap=new HashMap<>();

    private String passwordHash;

    @Override
    public String name() {
        return "config";
    }

    @Override
    public Future<Void> init(Vertx vertx, JsonObject config) {
        passwordHash = config.getString("password_hash","none");
        if (!(passwordHash.equals("md5")||passwordHash.equals("sh1")||passwordHash.equals("sha256")))
            passwordHash="none";
        JsonObject emptyJsonObject = new JsonObject();
        JsonArray user = config.getJsonArray("users");
        JsonArray acl = config.getJsonArray("acl");
        J.toJsonStream(user)
                .forEach(json->{
                    String username = json.getString("username");
                    String password = json.getString("password");
                    String ip = json.getString("ip");
                    JsonObject userProperty = json.getJsonObject("user_property",emptyJsonObject);
                    List<StringPair> pairs = userProperty.stream().map(e -> new StringPair(e.getKey(), e.getValue().toString())).collect(Collectors.toList());
                    User u = new User(username, password, ip,pairs);
                    userMap.put(u.userName,u);
                });

        J.toJsonStream(acl)
                .forEach(json->{
                    String clientId = json.getString("client");
                    JsonArray permission = json.getJsonArray("permission");
                    Permissions permissions = new Permissions();
                    J.toJsonStream(permission)
                            .forEach(j->{
                                String topic = j.getString("topic");
                                if (topic==null)
                                    return;
                                JsonObject userProperty = j.getJsonObject("user_property",emptyJsonObject);
                                List<StringPair> pairs = userProperty.stream().map(e -> new StringPair(e.getKey(), e.getValue().toString())).collect(Collectors.toList());
                                permissions.permissionMap.put(topic,Values3.values(j.getInteger("max_qos",2),j.getString("action"),pairs));
                            });
                    permissionMap.put(clientId,permissions);
                });

        return Future.succeededFuture();
    }

    @Override
    public Future<AuthResult<Boolean>> authorize(String clientId, MqttAuth auth,List<StringPair> userProperty) {
        String userName = auth.getUserName();
        Buffer passwordBytes = auth.getPassword();

        if (userName ==null)
            return Future.succeededFuture(AuthResult.create(false));

        User user = userMap.get(userName);
        if (user==null)
            return Future.succeededFuture(AuthResult.create(false));

        if (user.password!=null){
            String password = null;
            if (passwordHash.equals("md5")){
                password=md5(passwordBytes.getBytes());
            }else if (passwordHash.equals("sha1")){
                password=sha1(passwordBytes.getBytes());
            }else if (passwordHash.equals("sha256")){
                password=sha256(passwordBytes.getBytes());
            }else
                password=passwordBytes.toString(StandardCharsets.UTF_8);

            if (!user.password.equals(password))
                return Future.succeededFuture(AuthResult.create(false));
        }

        if (user.ip!=null){
            if (!user.ip.equals(auth.getSocketAddress().host()))
                return Future.succeededFuture(AuthResult.create(false));
        }

        return Future.succeededFuture(AuthResult.create(true).setPairList(user.list));
    }

    private String md5(byte[] bytes){
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            return new String(md5.digest(bytes),StandardCharsets.UTF_8);
        } catch (NoSuchAlgorithmException e) {
        }
        return null;
    }

    private String sha1(byte[] bytes){
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA1");
            byte[] digest = sha1.digest(bytes);
            StringBuilder sb = new StringBuilder();
            for (int i=0; i < digest.length; i++) {
                sb.append(Integer.toString(( digest[i] & 0xff ) + 0x100, 16).substring( 1 ));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
        }
        return null;
    }

    private String sha256(byte[] bytes){
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] digest = sha256.digest(bytes);
            StringBuilder sb = new StringBuilder();
            for (int i=0; i < digest.length; i++) {
                sb.append(Integer.toString(( digest[i] & 0xff ) + 0x100, 16).substring( 1 ));
            }
            return sb.toString();

        } catch (NoSuchAlgorithmException e) {
        }
        return null;
    }

    @Override
    public Future<AuthResult<Boolean>> authorizePub(String clientId, String topic) {
        if (permissionMap.isEmpty())
            return Future.succeededFuture(AuthResult.create(true));

        Permissions permissions = permissionMap.get(clientId);
        if (permissions==null)
            return Future.succeededFuture(AuthResult.create(false));

        Values3<Integer, String, List<StringPair>> values3 = permissions.permissionMap.get(topic);
        if (values3==null||!(values3.getTwo().equals("pub")||values3.getTwo().equals("both")))
            return Future.succeededFuture(AuthResult.create(false));


        return Future.succeededFuture(AuthResult.create(true).setPairList(values3.getThree()));
    }

    @Override
    public Future<AuthResult<List<ReasonCode>>> authorizeSub(String clientId, List<MqttSubscription> mqttSubscriptions,List<StringPair> userProperty) {

        Permissions permissions = permissionMap.get(clientId);
        if (permissions==null){
            return Future.succeededFuture(AuthResult.create(mqttSubscriptions
                    .stream().map(m->ReasonCode.NOT_AUTHORIZED).collect(Collectors.toList())));
        }

        Map<String,Values3<Integer,String,List<StringPair>>> permissionMap = permissions.permissionMap;
        List<StringPair> pairs=new ArrayList<>();
        List<ReasonCode> reasonCodes=mqttSubscriptions.stream().map(mqttSubscription -> {
            String topicFilter = mqttSubscription.getTopicFilter();
            Values3<Integer, String, List<StringPair>> values3 = permissionMap.get(topicFilter);
            //check action permit
            if (values3==null||!(values3.getTwo().equals("sub")||values3.getTwo().equals("both")))
                return ReasonCode.NOT_AUTHORIZED;
            //check max qos
            Integer maxQos = values3.getOne();
            int qos = mqttSubscription.getQos().value();
            if (maxQos< qos)
                qos=maxQos;
            pairs.addAll(values3.getThree());
            return ReasonCode.valueOf((byte) qos);
        }).collect(Collectors.toList());

        return Future.succeededFuture(AuthResult.create(reasonCodes).setPairList(pairs));
    }

    private class Permissions{
        private Map<String, Values3<Integer,String,List<StringPair>>> permissionMap;


        public Permissions() {
            permissionMap=new HashMap<>();
        }

    }

    private class User {
        private String userName;
        private String password;
        private String ip;
        private List<StringPair> list;

        public User(String userName, String password, String ip, List<StringPair> list) {
            this.userName = userName;
            this.password = password;
            this.ip = ip;
            this.list = list;
        }
    }

}
