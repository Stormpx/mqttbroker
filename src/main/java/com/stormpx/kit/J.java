package com.stormpx.kit;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Collections;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class J {

    public final static JsonArray EMPTY_ARRAY=new JsonArray(Collections.emptyList());


    public static <T> Collector<T, JsonArray, JsonArray> toJsonArray(){
        return Collector.of(JsonArray::new, JsonArray::add, JsonArray::addAll, Collector.Characteristics.IDENTITY_FINISH);
    }

    public static Stream<JsonObject> toJsonStream(JsonArray jsonArray){
        return jsonArray.stream().filter(o->o instanceof JsonObject).map(o->(JsonObject)o);
    }

}
