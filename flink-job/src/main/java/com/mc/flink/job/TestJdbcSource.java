package com.mc.flink.job;

import com.alibaba.fastjson.JSONObject;
import com.mc.utils.YmlUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author macheng
 * @date 2022/7/28 15:49
 */
public class TestJdbcSource {
    public static void main(String[] args) {

        YmlUtil.init("~/application.yml");

        JSONObject jdbcConfig = JSONObject.parseObject(YmlUtil.get("mysql"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        BasicTypeInfo[] basicTypeInfos = new BasicTypeInfo[1];
        basicTypeInfos[0] = BasicTypeInfo.LONG_TYPE_INFO;
        RowTypeInfo rowTypeInfo = new RowTypeInfo(basicTypeInfos);

        DataStreamSource<Row> input = env.createInput(JdbcInputFormat.buildJdbcInputFormat()
                .setDBUrl(jdbcConfig.getString("url"))
                .setDrivername(jdbcConfig.getString("driver"))
                .setUsername(jdbcConfig.getString("username"))
                .setPassword(jdbcConfig.getString("password"))
                .setQuery("select id from t_cdc order by id")
                .setRowTypeInfo(rowTypeInfo)
                .finish());
        env.enableCheckpointing(1000);
        input.map(Row::toString).assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<String>) (element, recordTimestamp) -> System.currentTimeMillis()))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {

                    @Override
                    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
                        if (iterable == null) {
                            return;
                        }
                        for (String s : iterable) {
                            collector.collect(s);
                        }
                    }
                }).print();
    }
}
