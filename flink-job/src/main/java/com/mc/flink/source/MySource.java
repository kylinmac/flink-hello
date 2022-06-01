package com.mc.flink.source;

import com.mc.flink.api.BaseFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class MySource extends RichSourceFunction<String> {



    int i=0;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        ClassLoader classLoader = MySource.class.getClassLoader();
        Class<?> sourceClass = classLoader.loadClass("com.mc.flink.func.SourceFunction");
        Object o = sourceClass.getConstructor().newInstance();
        if (o instanceof BaseFunction){
            System.out.println(" class check success ==============");
        }
        System.out.println("source classloader==============="+classLoader);
        while (i<10){
            String apply = (String)sourceClass.getMethod("apply").invoke(o);
            sourceContext.collect(apply);
        }
        sourceContext.close();
    }

    @Override
    public void cancel() {
        i=10;
    }
}
