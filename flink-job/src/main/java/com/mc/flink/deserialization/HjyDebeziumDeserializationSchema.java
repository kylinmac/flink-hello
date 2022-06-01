package com.mc.flink.deserialization;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.List;


public class HjyDebeziumDeserializationSchema implements DebeziumDeserializationSchema<JSONObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HjyDebeziumDeserializationSchema.class);

    private static final long serialVersionUID = 1L;
    private static final LocalDate startDate = LocalDate.of(1970, 1, 1);

    public HjyDebeziumDeserializationSchema() {
    }

    /**
     * @param sourceRecord sourceRecord
     * @param collector    out
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<JSONObject> collector) {
        JSONObject resJson = new JSONObject();
        try {
            Struct valueStruct = (Struct) sourceRecord.value();
            Struct afterStruct = valueStruct.getStruct("after");
            Struct beforeStruct = valueStruct.getStruct("before");
            Struct sourceStruct = valueStruct.getStruct("source");
            JSONObject data = new JSONObject();
            // 注意：若valueStruct中只有after,则表明插入；若只有before，说明删除；若既有before，也有after，则代表更新
            if (afterStruct != null) {
                // 插入 or 更新
                processFields(afterStruct, data);
                //设置删除字段属性
                data.put("is_delete_doris", 0);
            }

            if (afterStruct == null && beforeStruct != null) {
                processFields(beforeStruct, data);
                //设置删除字段属性
                data.put("is_delete_doris", 1);

            }
            resJson.put("data", data);
            resJson.put("table", sourceStruct.get("table"));
        } catch (Exception e) {
            LOGGER.error("Deserialize throws exception:", e);
        }
        collector.collect(resJson);
    }

    private void processFields(Struct afterStruct, JSONObject data) {
        List<Field> fields = afterStruct.schema().fields();
        String name;
        Object value;
        for (Field field : fields) {
            name = field.name();
            value = afterStruct.get(name);

            if ("io.debezium.time.Date".equals(field.schema().name()) && value != null) {
                value = startDate.plusDays((int) value);
            }
            if ("io.debezium.time.Timestamp".equals(field.schema().name()) && value != null) {
                value = LocalDateTime.ofInstant(Instant.ofEpochMilli((long) value), ZoneId.of("UTC"));
            }
            if ("io.debezium.time.ZonedTimestamp".equals(field.schema().name()) && value != null) {
                value = ZonedDateTime.parse((String) value).toLocalDateTime();
            }

            data.put(name, value);
        }
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return BasicTypeInfo.of(JSONObject.class);
    }
}

