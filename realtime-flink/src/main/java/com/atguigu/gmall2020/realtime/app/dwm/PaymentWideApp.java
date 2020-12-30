package com.atguigu.gmall2020.realtime.app.dwm;

import com.alibaba.fastjson.JSON;

import com.atguigu.gmall2020.realtime.bean.*;
import com.atguigu.gmall2020.realtime.utils.DateTimeUtil;
import com.atguigu.gmall2020.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class PaymentWideApp {

    public static void main(String[] args) {

        //定义环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/PaymentWideApp");
        env.setStateBackend(fsStateBackend);

        //接收数据流
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "DWD_PAYMENT_INFO";
        String orderWideSourceTopic = "DWM_ORDER_WIDE";

        String paymentWideSinkTopic = "DWM_PAYMENT_WIDE";

        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        DataStream<String> paymentInfojsonDstream =
                env.addSource(paymentInfoSource);
        DataStream<PaymentInfo> paymentInfoDStream =
                paymentInfojsonDstream.map(jsonString -> JSON.parseObject(jsonString, PaymentInfo.class));

        FlinkKafkaConsumer<String> orderWideSource =
                MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStream<String> orderWidejsonDstream =
                env.addSource(orderWideSource);
        DataStream<OrderWide> orderWideDstream =
                orderWidejsonDstream.map(jsonString -> JSON.parseObject(jsonString, OrderWide.class));

        //设置水位线
        SingleOutputStreamOperator<PaymentInfo> paymentInfoEventTimeDstream =
                paymentInfoDStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (paymentInfo, ts) -> DateTimeUtil.toTs(paymentInfo.getCallback_time())
                                ));

        SingleOutputStreamOperator<OrderWide> orderInfoWithEventTimeDstream =
                orderWideDstream.assignTimestampsAndWatermarks(WatermarkStrategy.
                        <OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                (orderWide, ts) -> DateTimeUtil.toTs(orderWide.getCreate_time())
                        )
                );

        //设置分区键
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream =
                paymentInfoEventTimeDstream.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedStream =
                orderInfoWithEventTimeDstream.keyBy(OrderWide::getOrder_id);

        //关联数据
        SingleOutputStreamOperator<PaymentWide> paymentWideSingleOutputStreamOperator =
                paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream).
                        between(Time.seconds(-1800), Time.seconds(0)).
                        process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo,
                                                       OrderWide orderWide,
                                                       Context ctx, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }).uid("payment_wide_join");

        paymentWideSingleOutputStreamOperator.addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
