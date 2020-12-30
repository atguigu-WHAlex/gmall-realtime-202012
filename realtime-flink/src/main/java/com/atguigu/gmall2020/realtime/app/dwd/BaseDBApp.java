package com.atguigu.gmall2020.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


import com.atguigu.gmall2020.realtime.app.func.*;
import com.atguigu.gmall2020.realtime.bean.TableProcess;
import com.atguigu.gmall2020.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;


public class BaseDBApp {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String groupId = "ods_order_detail_group";
        String topic = "ODS_BASE_DB_M";
        FlinkKafkaConsumer<String> source = MyKafkaUtil.getKafkaSource(topic, groupId);

        DataStream<String> jsonDstream = env.addSource(source);
        jsonDstream.print("data json:::::::");
        DataStream<JSONObject> dataStream = jsonDstream.map(JSON::parseObject);

        //过滤data为空或者长度不足的数据
        SingleOutputStreamOperator<JSONObject> filteredDstream = dataStream.filter(jsonObject -> {
            return jsonObject.getString("table") != null && jsonObject.getString("data") != null && jsonObject.getString("data").length() > 3;
        });
        filteredDstream.print("json::::::::");

        //设置分流标签
        final OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };

        SingleOutputStreamOperator<JSONObject> kafkaStream = filteredDstream.process(new TableProcessFunction(hbaseTag));

        DataStream<JSONObject> hbaseStream = kafkaStream.getSideOutput(hbaseTag);
        hbaseStream.print("hbase::::");
        hbaseStream.addSink(new DimSink());
        kafkaStream.print("kafka ::::");
        FlinkKafkaProducer kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("启动 kafka sink");
            }

            /*
            从每条数据的得到该条数据应送往的主题名
             */
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                String topic = jsonObject.getString("sink_table").toUpperCase();
                JSONObject dataJsonObj = jsonObject.getJSONObject("data");
                return new ProducerRecord(topic, dataJsonObj.toJSONString().getBytes());
            }

        });

        kafkaStream.addSink(kafkaSink);


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("异常");
        }
    }

}
