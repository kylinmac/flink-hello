package com.mc.flink.func;

import com.mc.flink.api.BaseFunction;

import java.util.concurrent.atomic.AtomicLong;

public class SourceFunction implements BaseFunction {



    @Override
    public String apply() {

        return "source";

    }
}
