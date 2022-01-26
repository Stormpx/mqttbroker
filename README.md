mqttbroker
=================
vertx写的mqtt broker



特性
-----------
* 支持MQTT v3.1.1 & v5
* 支持Qos 0-2
* 支持Retain&Will
* 支持SSL/TLS
* 支持订阅通配符&共享订阅
* 支持MQTT over Websocket
* 支持消息&会话持久化
* 支持认证&授权

配置
-----------

```yaml
sni: false
tcp_no_delay: false

verticle_instance: 6

mqtt:

  max_message_expiry_interval: 6000
  max_session_expiry_interval: 6000
  maximum_qos: 2
  #byte
  maximum_packet_size: 268435455
  receive_maximum: 65535
  topic_alias_maximum: 65535
  retain_available: true
  #second
  server_keep_alive: 10
  wildcard_subscription_available: true
  subscription_identifier_available: true
  shared_subscription_available: true

tcp:
  enable: true
  host: 0.0.0.0
  port: 11883
  ssl: false
  key_cert:
    - key_file: /privkey.pem
      cert_file: /cert.pem

ws:
  enable: true
  host: 0.0.0.0
  port: 18183
  path: /mqtt
  ssl: false
  key_cert:
    - key_file: /privkey.pem
      cert_file: /cert.pem

log_level: info
log_dir: /log

save_dir: /foo
```
认证 鉴权
-----------
通过配置`auth`选择认证的方式 默认是`anonymous`
```yaml
auth: config|http|eventbus|redis
```


config认证 鉴权
--- 
`password_hash` 密码的hash算法
```yaml
password_hash: none|md5|sha|sha256
```
### config认证 
必填 `username`  
可选 `password`  
可选`ip`不为空时会参与验证    
可选 `user_property` 用户属性 认证成功并且协议版本是MQTTv5时会添加进CONACK里返回 
```yaml
users:
  - username: user1
    password: password
    ip: 0.0.0.0
    user_property:
      key: value

  - username: user2
```

### config鉴权
`max_qos`在SUBSCIBE中限制订阅等级  
`user_property`鉴权通过会在PUBACK/PUBREC/SUBACK中返回到客户端
```yaml
acl:
  - client: client1
    permission:
      - topic: topic1
        max_qos: 2
        action: pub|sub|both
        user_property:
          key: value
      - topic: topic2
        action: pub

```

http认证 鉴权
---
请求统一使用POST  状态码2xx时代表通过 4xx代表拒绝 其他代表请求失败  
`appkey`不为空时会带着请求  
 `http_authentication_url` 登陆认证url  
  `http_authorization_url` pub/sub 授权url
```yaml
appkey: appkey
http_authentication_url: 'https://localhost/authentication'
http_authorization_url: 'https://localhost/authorization'
user_agent: 'UA/1.0'
```
### 登陆认证url
请求体 application/json
```json
{
  "client": "client1",
  "username": "username",
  "password": "password",
  "ip": "127.0.0.1",
  "appkey": "qppkey",
  "user_property": {
        "key": "value"
   }
}
```
响应  可以不包含body 如果有响应`user_property`并且协议版本是MQTTv5的情况 会在CONACK返回
```
2xx/4xx

contentType: application/json

{
 "user_property": {
    "key": "value"
  }
}
```

### PUB/SUB 鉴权url

#####PUB 请求体
```json
{
  "action" : "pub",
  "clientId" : "client1",
  "topic" : "/test/2",
  "appkey" : "appkey"
}
```
#####PUB 响应  
可以不包含body 如果有响应`user_property`并且协议版本是MQTTv5的情况 会在PUBACK/PUBREC返回 
```
2xx/4xx

contentType: application/json

{
 "user_property": {
    "key": "value"
 }
}
```
___
#####SUB 请求体
```json
{
  "action" : "sub",
  "clientId" : "client1",
  "topicFilters" : [ {
    "topic" : "/test/4",
    "qos" : 2
  }, {
    "topic" : "/test/5",
    "qos" : 2
  } ],
  "appkey" : "appkey",
  "user_property": {
      "key": "value"
  }
}
```
#####SUB 响应  
 2xx代表通过 `topicFilters`为空代表全部通过 `topicFilters[n].topic!=null&&topicFilters[n].qos==null`代表拒绝此topic订阅  
 4xx代表全部拒绝 会忽略body里面的`topicFilters`  
 如果有响应`user_property`并且协议版本是MQTTv5的情况 会在SUBACK返回
```
2xx/4xx

contentType: application/json

{
 "topicFilters" : [ {
     "topic" : "/test/4"
   }, {
     "topic" : "/test/5",
     "qos" : 2
   } ],
   "user_property": {
       "key": "value"
   }
}
```