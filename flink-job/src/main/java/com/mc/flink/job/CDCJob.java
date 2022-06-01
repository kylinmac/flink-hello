package com.mc.flink.job;

import com.alibaba.fastjson.JSONObject;
import com.mc.utils.YmlUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import  com.mc.flink.deserialization.HjyDebeziumDeserializationSchema;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CDCJob {
    public static void main(String[] args) throws Exception {
        YmlUtil.init("/" + args[0] + "/application-" + args[1] + ".yml");
        String serverId = YmlUtil.getStr("serverId");
        String jobName = args[1] + " " + serverId + " sync:";
        Properties p = new Properties();
        ArrayList<Map<String, Object>> sourceProperties = YmlUtil.get("datasources");

        ConcurrentHashMap<String, MySqlSource<JSONObject>> sources = new ConcurrentHashMap<>();
        for (Map<String, Object> sourceProperty : sourceProperties) {
            sources.put((String) sourceProperty.get("datasource"), MySqlSource.<JSONObject>builder()
                    .hostname((String) sourceProperty.get("host"))
                    .port((Integer) sourceProperty.get("port"))
                    .databaseList((String) sourceProperty.get("databaseList"))

                    .tableList((String) sourceProperty.get("tableList"))
                    .username((String) sourceProperty.get("username"))
                    .password(sourceProperty.get("password") + "")
                    .connectTimeout(Duration.ofSeconds(3600))
                    .serverId(serverId)
                    //指定从binlog最新位置开始同步
//                    .startupOptions(StartupOptions.latest())
                    .debeziumProperties(p)
                    .deserializer(new HjyDebeziumDeserializationSchema())
                    //获取DDL暂不启用
                    //.includeSchemaChanges(true)
                    .build());
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<SingleOutputStreamOperator<JSONObject>> dataStreamSources = new ArrayList<>();
        for (Map.Entry<String, MySqlSource<JSONObject>> source : sources.entrySet()) {
            dataStreamSources.add(
                    env
                            .fromSource(source.getValue(), WatermarkStrategy.noWatermarks(), "MySQL Source " + source.getKey())
                            .uid(jobName + "syncdatabase" + source.getKey())
            );
        }
        // enable checkpoint
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);

        //多源合并
        Optional<DataStream<JSONObject>> collect = dataStreamSources.stream()
                .collect(Collectors.reducing(DataStream::union));
        collect.ifPresent(DataStream::print);

        env.execute(" MySQL CDC " + jobName + " ");
    }
}
