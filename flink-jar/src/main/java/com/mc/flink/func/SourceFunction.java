package com.mc.flink.func;

import com.mc.flink.api.BaseFunction;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class SourceFunction implements BaseFunction, Serializable {



    @Override
    public String apply() {

        return "source";

    }
}
