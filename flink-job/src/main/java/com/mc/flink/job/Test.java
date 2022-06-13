package com.mc.flink.job;

import java.util.Date;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Test {
    public static void main(String[] args) {

        Integer num=null;
        IntStream.of(1,2,3).forEach(x->{
            System.out.println(num);
        });

    }
}
