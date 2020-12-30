package com.atguigu.gmall2020.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

public class MyKafkaUtil {

    static String kafkaServer = "hdp1:9092,hdp2:9092,hdp3:9092";

    static final String DEFAULT_TOPIC = "DEFAULT_DATA";

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties prop = new Properties();
        prop.setProperty("group.id", groupId);
        prop.setProperty("bootstrap.servers", kafkaServer);
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
    }

    public static FlinkKafkaProducer getKafkaSink(String topic) {
        return new FlinkKafkaProducer(kafkaServer, topic, new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> serializationSchema) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", kafkaServer);

        return new FlinkKafkaProducer<>(DEFAULT_TOPIC, serializationSchema, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static String getKafkaDDL(String topic, String groupId) {

        return "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + kafkaServer + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";
    }

}
