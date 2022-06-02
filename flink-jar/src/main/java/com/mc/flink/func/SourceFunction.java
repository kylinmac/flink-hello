package com.mc.flink.func;

import com.mc.flink.api.BaseFunction;
import com.mc.flink.udf.TestClassPath;

import java.io.Serializable;
import java.time.LocalDateTime;

public class SourceFunction implements BaseFunction, Serializable {



    @Override
    public String apply() {
        System.out.println(LocalDateTime.now()+" source :");
        TestClassPath testClassPath = new TestClassPath();
        testClassPath.output();
        return "source";

    }
}
