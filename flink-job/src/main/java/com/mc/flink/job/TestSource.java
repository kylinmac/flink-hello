package com.mc.flink.job;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;


public class TestSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> sourceStream = env.addSource(new MySource());
        env.setParallelism(1);
//        env.enableCheckpointing(3000L);
        AtomicLong atomicLong = new AtomicLong(0);
        SingleOutputStreamOperator<Long> process = sourceStream.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long aLong, ProcessFunction<Long, Long>.Context context, Collector<Long> collector) throws Exception {
                if (aLong<atomicLong.getAndIncrement()){
                    throw new RuntimeException(" wrong order ");
                }
                context.output(new OutputTag<>(aLong % 256 + "", TypeInformation.of(Long.class)), aLong);
            }
        }).setParallelism(8);
        process.getSideOutput(new OutputTag<>(1+"",TypeInformation.of(Long.class))).print().setParallelism(1);
        env.execute("test save");

    }


    static class MySource extends RichSourceFunction<Long> implements CheckpointedFunction {
        ListState<JSONObject> checkpointedCount;
        private volatile boolean isRunning = true;
        private long count = 0L;
        private JSONObject param;

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            System.out.println(" save state ");
            this.checkpointedCount.clear();
            this.checkpointedCount.add(param);
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            this.checkpointedCount = functionInitializationContext
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("restore", JSONObject.class));

            if (functionInitializationContext.isRestored()) {
                System.out.println(" restore ");
                for (JSONObject json : this.checkpointedCount.get()) {
                    System.out.println(" restore param: "+json);
                  this.param = json;
              }
            }
        }

        @Override
        public void run(SourceContext<Long> sourceContext) throws Exception {
            while (isRunning&& count<10000) {
                synchronized (sourceContext.getCheckpointLock()) {
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("key", count);
                    param = jsonObject;
                    sourceContext.collect(count++);
                }
            }
            System.out.println("close");
            sourceContext.close();
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
