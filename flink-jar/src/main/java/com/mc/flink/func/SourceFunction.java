package com.mc.flink.func;

import com.mc.flink.api.BaseFunction;

import java.util.concurrent.atomic.AtomicLong;

public class SourceFunction implements BaseFunction {

    AtomicLong atomicLong=new AtomicLong(0);

    @Override
    public String apply() {

        return atomicLong.getAndIncrement()+"";

    }
}
