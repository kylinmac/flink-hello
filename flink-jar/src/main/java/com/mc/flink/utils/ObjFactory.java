package com.mc.flink.utils;

import com.alibaba.fastjson.JSONObject;

public class ObjFactory {
    public static JSONObject getJson(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("key","success version 4");
        return jsonObject;
    }
}
