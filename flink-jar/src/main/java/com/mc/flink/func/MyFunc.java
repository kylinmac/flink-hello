package com.mc.flink.func;

import com.mc.flink.utils.ObjFactory;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class MyFunc extends ProcessFunction<String, String> implements Serializable {

    @Override
    public void processElement(String string, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
        collector.collect(ObjFactory.getJson().toString());
    }

}
