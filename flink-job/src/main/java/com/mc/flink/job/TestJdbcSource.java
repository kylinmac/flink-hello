package com.mc.flink.job;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mc.utils.YmlUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.time.Duration;

/**
 * @author macheng
 * @date 2022/7/28 15:49
 */
public class TestJdbcSource {
    public static void main(String[] args) throws Exception {

        YmlUtil.init("~/application.yml");

        JSONObject jdbcConfig = JSONObject.parseObject(YmlUtil.get("mysql"));
        jdbcConfig.put("cols", "id");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        BasicTypeInfo[] basicTypeInfos = new BasicTypeInfo[1];
//        basicTypeInfos[0] = BasicTypeInfo.LONG_TYPE_INFO;
//        RowTypeInfo rowTypeInfo = new RowTypeInfo(basicTypeInfos);
//
//        DataStreamSource<Row> input = env.createInput(JdbcInputFormat.buildJdbcInputFormat()
//                .setDBUrl(jdbcConfig.getString("url"))
//                .setDrivername(jdbcConfig.getString("driver"))
//                .setUsername(jdbcConfig.getString("username"))
//                .setPassword(jdbcConfig.getString("password"))
//                .setQuery("select id from t_cdc order by id")
//                .setRowTypeInfo(rowTypeInfo)
//                .finish());
        DataStreamSource<JSONObject> input = env.addSource(new JdbcSourceFunction("select id from t_cdc limit 100000", jdbcConfig));
        env.enableCheckpointing(1000);
        input.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1000000))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> System.currentTimeMillis()))
                .windowAll(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<JSONObject, JSONObject, TimeWindow>() {

                    @Override
                    public void process(ProcessAllWindowFunction<JSONObject, JSONObject, TimeWindow>.Context context, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
                        if (iterable == null) {
                            return;
                        }
                        int all=0;
                        for (JSONObject s : iterable) {
                            all++;
                        }
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("total",all);
                        collector.collect(jsonObject );
                        System.out.println("=================================total: "+all);
                    }
                }).print();

        env.execute();
    }

    static class JdbcSourceFunction extends RichSourceFunction<JSONObject> implements CheckpointedFunction {
        private String sql;
        private JSONArray data;
        private boolean flag = true;
        private JSONObject jdbcConfig;
        private int total = 0;

        private int current = 0;
        private int retryTimes = 0;
        ListState<JSONObject> checkpointState;

        public JdbcSourceFunction(String sql, JSONObject jdbcConfig) {
            this.sql = sql;
            this.jdbcConfig = jdbcConfig;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            this.checkpointState.clear();
            JSONObject state = new JSONObject();
            state.put("sql", sql);
            state.put("total", total);
            state.put("current", current);
            state.put("jsonArray", data);
            state.put("retryTimes", retryTimes + 1);
            state.put("jdbcConfig", jdbcConfig);
            this.checkpointState.add(state);
            System.out.println("===============save source current:"+current);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.checkpointState = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("restore", JSONObject.class));

            if (context.isRestored()) {
                for (JSONObject json : this.checkpointState.get()) {
                    this.total = json.getIntValue("total");
                    this.sql = json.getString("sql");
                    this.retryTimes = json.getIntValue("retryTimes");
                    this.current = json.getIntValue("current");
                    this.data = json.getJSONArray("jsonArray");
                    this.jdbcConfig = json.getJSONObject("jdbcConfig");
                }
            }
            System.out.println("===============init source current:"+current);
            System.out.println("===============init source retryTimes:"+retryTimes);
        }

        @Override
        public void run(SourceContext<JSONObject> ctx) throws Exception {
            if (data == null) {
                System.out.println("===============excute :"+current);
                data = executeQuery(jdbcConfig, sql);
                total = data.size();
            }
            System.out.println("===============start source current:"+current);
            while (current < total && flag) {
                if (current==50000){
                    int i=1/0;
                }
                ctx.collectWithTimestamp((JSONObject) data.get(current++), System.currentTimeMillis());
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    public static JSONArray executeQuery(JSONObject db, String sql, Object... bindArgs) throws ClassNotFoundException, SQLException {

        String driverClassName = "com.mysql.cj.jdbc.Driver";
        String url = db.getString("url");
        String username = db.getString("username");
        String password = db.getString("password");
        String[] cols = db.getString("cols").split(",");
        //加载驱动
        Class.forName(driverClassName);

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = DriverManager.getConnection(url, username, password);
            JSONArray arr = new JSONArray();


            /**获取数据库连接池中的连接**/
            preparedStatement = connection.prepareStatement(sql);
            if (bindArgs != null) {
                /**设置sql占位符中的值**/
                for (int i = 0; i < bindArgs.length; i++) {
                    preparedStatement.setObject(i + 1, bindArgs[i]);
                }
            }
            /**执行sql语句，获取结果集**/
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                JSONObject obj = new JSONObject();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    obj.put(cols[i - 1], resultSet.getObject(i));
                }
                arr.add(obj);
            }
            return arr;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
                connection = null;
            }
        }

    }

}
