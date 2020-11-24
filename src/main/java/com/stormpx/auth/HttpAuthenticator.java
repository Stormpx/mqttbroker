package com.stormpx.auth;

import com.stormpx.message.MqttAuth;
import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.mqtt.ReasonCode;
import com.stormpx.kit.J;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HttpAuthenticator implements Authenticator {
    private Vertx vertx;
    private String appKey;
    private String authenticationUrl;
    private String authorizationUrl;
    private String userAgent;
    private WebClient webClient;
    @Override
    public String name() {
        return "http";
    }

    @Override
    public Future<Void> init(Vertx vertx, JsonObject config) {
        this.appKey=config.getString("appkey");
        String authentication_url = config.getString("http_authentication_url");
        String authorization_url = config.getString("http_authorization_url");

        try {
            URL url = new URL(authentication_url);
        } catch (MalformedURLException e) {
            return Future.failedFuture("Invalid authentication url"+authentication_url);
        }
        try {
            URL url = new URL(authorization_url);
        } catch (MalformedURLException e) {
            return Future.failedFuture("Invalid authorization url"+authentication_url);
        }
        this.authenticationUrl=authentication_url;
        this.authorizationUrl=authorization_url;
        this.userAgent=config.getString("user_agent","mqttUa");
        this.webClient=WebClient.create(vertx,new WebClientOptions().setTryUseCompression(true).setUserAgentEnabled(true).setUserAgent(this.userAgent));
        return Future.succeededFuture();
    }

    @Override
    public Future<AuthResult<Boolean>> authorize(String clientId, MqttAuth auth,List<StringPair> userProperty) {

        HttpRequest<Buffer> httpRequest = webClient.postAbs(authenticationUrl);
        JsonObject params = new JsonObject();
        params.put("clientId",clientId);
        if (auth.getUserName()!=null)
            params.put("username",auth.getUserName());
        String password = auth.getPassword().toString(StandardCharsets.UTF_8);
        params.put("password",password);
        params.put("ip",auth.getSocketAddress().host());
        if (userProperty!=null)
            params.put("user_property",userProperty);
        if (appKey!=null)
            params.put("appkey",appKey);

        return authReq(params,httpRequest);

    }

    private Future<AuthResult<Boolean>> authReq(JsonObject jsonObject, HttpRequest<Buffer> httpRequest){
        Promise<AuthResult<Boolean>> promise=Promise.promise();
        httpRequest.sendJsonObject(jsonObject,ar->{
            if (ar.succeeded()){
                HttpResponse<Buffer> response = ar.result();
                String contentType = response.headers().get(HttpHeaders.CONTENT_TYPE);
                List<StringPair> userProperty=null;
                if (contentType!=null&&contentType.contains("json")) {
                    JsonObject json = response.bodyAsJsonObject();
                    userProperty = getUserProperty(json);
                }
                AuthResult<Boolean> authResult=null;
                if (response.statusCode()>=200&&response.statusCode()<300){
                    //success
                    authResult = AuthResult.create(true);
                }else if (response.statusCode()>=400&&response.statusCode()<500){
                    authResult = AuthResult.create(false);
                }else{
                    promise.fail(response.statusMessage());
                    return;
                }

                authResult.setPairList(userProperty);

                promise.complete(authResult);
            }else{
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }

    @Override
    public Future<AuthResult<Boolean>> authorizePub(String clientId, String topic) {

        HttpRequest<Buffer> httpRequest = webClient.postAbs(authorizationUrl);
        JsonObject params = new JsonObject();
        params.put("clientId",clientId);
        params.put("topic",topic);
        params.put("action","pub");
        if (appKey!=null)
            params.put("appkey",appKey);

        return authReq(params,httpRequest);
    }

    @Override
    public Future<AuthResult<List<ReasonCode>>> authorizeSub(String clientId, List<MqttSubscription> mqttSubscriptions,List<StringPair> userProperty) {

        HttpRequest<Buffer> httpRequest = webClient.postAbs(authorizationUrl);
        List<JsonObject> list = mqttSubscriptions.stream()
                .map(mqttSubscription -> new JsonObject()
                        .put("topic", mqttSubscription.getTopicFilter())
                        .put("qos", mqttSubscription.getQos().value()))
                .collect(Collectors.toList());
        JsonObject params = new JsonObject();
        if (userProperty!=null)
            params.put("user_property",userProperty.stream().reduce(new JsonObject(),(j,stringPair)-> j.put(stringPair.getKey(),stringPair.getValue()),(q, w)->q));
        params.put("clientId",clientId);
        params.put("topicFilters",list);
        params.put("action","sub");
        if (appKey!=null)
            params.put("appkey",appKey);
        Promise<AuthResult<List<ReasonCode>>> promise=Promise.promise();

        httpRequest.sendJsonObject(params,ar->{
            if (ar.succeeded()){
                HttpResponse<Buffer> response = ar.result();
                String contentType = response.headers().get(HttpHeaders.CONTENT_TYPE);

                List<StringPair> respUserProperty=null;
                Map<String,Integer> map=new HashMap<>();
                if (contentType!=null&&contentType.contains("json")) {
                    JsonObject json = response.bodyAsJsonObject();
                    respUserProperty = getUserProperty(json);

                    // topic array topic!=nul&&(qos==null||qos<0||qos>2)  mean not authorized
                    JsonArray topic = json.getJsonArray("topicFilters");
                    if (topic!=null) {
                        J.toJsonStream(topic).forEach(j -> map.put(j.getString("topic"), j.getInteger("qos")));
                    }
                }

                if (response.statusCode()>=200&&response.statusCode()<300){
                    //success
                    if (contentType!=null&&contentType.contains("json")) {

                        List<ReasonCode> reasonCodes=mqttSubscriptions.stream().map(mqttSubscription -> {
                            if (!map.containsKey(mqttSubscription.getTopicFilter())){
                                return ReasonCode.valueOf((byte) mqttSubscription.getQos().value());
                            }
                            Integer qos = map.get(mqttSubscription.getTopicFilter());
                            if (qos==null||qos<0||qos>2)
                                return ReasonCode.NOT_AUTHORIZED;

                            return ReasonCode.valueOf(qos.byteValue());

                        }).collect(Collectors.toList());
                        AuthResult<List<ReasonCode>> authResult = AuthResult.create(reasonCodes).setPairList(respUserProperty);
                        promise.complete(authResult);
                    }else{
                        promise.complete(AuthResult.create(mqttSubscriptions.stream().map(m->ReasonCode.valueOf((byte) m.getQos().value())).collect(Collectors.toList())));
                    }

                }else if (response.statusCode()>=400&&response.statusCode()<500){
                    AuthResult<List<ReasonCode>> authResult = AuthResult.create(mqttSubscriptions.stream().map(m -> ReasonCode.NOT_AUTHORIZED).collect(Collectors.toList()));
                    authResult.setPairList(respUserProperty);
                    promise.complete(authResult);
                }else{
                    promise.fail(response.statusMessage());
                }
            }else{
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }


    private List<StringPair> getUserProperty(JsonObject json){
        JsonObject userProperty = json.getJsonObject("user_property");
        if (userProperty != null) {
            return userProperty.stream().map(e -> new StringPair(e.getKey(), e.getValue().toString())).collect(Collectors.toList());
        }
        return null;
    }

}
