package com.mc.flink.job;



import com.mc.flink.api.BaseFunction;
import com.mc.flink.source.MySource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkUserCodeClassLoader;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class HelloJob {

    public static void main(String[] args) throws Exception {
        for (String arg : args[0].split(",")) {
            loadJar(new URL(arg));
        }
        ClassLoader classLoader = HelloJob.class.getClassLoader();
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.addSource(new MySource(Integer.parseInt(args[2])));


        Class<?> aClass = classLoader.loadClass("com.mc.flink.func.MyFunc");

        setClasspath(args[1].split(","), env);
        ProcessFunction<String, String> instance = (ProcessFunction<String, String>)aClass.getConstructor().newInstance();
        dataStreamSource.process(instance).flatMap(new RichFlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                RuntimeContext runtimeContext = getRuntimeContext();
                URLClassLoader userCodeClassLoader = (URLClassLoader)runtimeContext.getUserCodeClassLoader();
                System.out.println(LocalDateTime.now()+" task thread==============================:"+Thread.currentThread().getContextClassLoader());
                System.out.println(LocalDateTime.now()+" task==========================:"+userCodeClassLoader);
                System.out.println(LocalDateTime.now()+" paths start==========================:");
                for (URL url : userCodeClassLoader.getURLs()) {
                    System.out.println(url.getPath());
                }
                System.out.println(LocalDateTime.now()+" paths end==========================:");
                collector.collect(s);
                collector.collect(s+"flat");
            }
        }).print();

        env.execute();
    }

    private static void setClasspath(String[] args, StreamExecutionEnvironment env) throws NoSuchFieldException, IllegalAccessException {
        Field configuration = StreamExecutionEnvironment.class.getDeclaredField("configuration");
        configuration.setAccessible(true);
        Configuration o = (Configuration)configuration.get(env);
        Field confData = Configuration.class.getDeclaredField("confData");
        confData.setAccessible(true);
        Map<String,Object> temp = (Map<String,Object>)confData.get(o);
        List<String> jarList = new ArrayList<>();
        for (String arg : args) {
            jarList.add(arg);
        }
        temp.put("pipeline.classpaths",jarList);
    }

    public static void loadJar(URL jarUrl) {
        //从URLClassLoader类加载器中获取类的addURL方法
        Method method = null;
        try {
            method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException | SecurityException e1) {
            e1.printStackTrace();
        }
        // 获取方法的访问权限
        boolean accessible = method.isAccessible();
        try {
            //修改访问权限为可写
            if (accessible == false) {
                method.setAccessible(true);
            }
            // 获取系统类加载器
            ClassLoader classLoader = HelloJob.class.getClassLoader();
            System.out.println(LocalDateTime.now()+" job thread==============================:"+Thread.currentThread().getContextClassLoader());
            System.out.println(LocalDateTime.now()+" job==============================:"+classLoader);
            //jar路径加入到系统url路径里
            method.invoke(classLoader, jarUrl);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    interface  Inter{
       void consume();
    }

    class DyH implements InvocationHandler{
            class DyInter implements Inter{

                @Override
                public void consume() {
                    System.out.println("DyInter");
                }
            }
            Inter i=new DyInter();
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            System.out.println("invoke");
            return method.invoke(i,args);
        }
    }

}
