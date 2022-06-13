package com.mc.flink.func;

import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.mc.flink.udf.TestClassPath;
import com.mc.flink.utils.ObjFactory;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.LocalDateTime;

public class MyFunc extends ProcessFunction<String, String> implements Serializable {

    @Override
    public void processElement(String string, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
        TestClassPath testClassPath = new TestClassPath();
        System.out.println(LocalDateTime.now()+" process :");
        testClassPath.output();
        collector.collect(ObjFactory.getJson().toString());
        String s="test_camel";
        StringUtils.underlineToCamel(s);
    }

}
