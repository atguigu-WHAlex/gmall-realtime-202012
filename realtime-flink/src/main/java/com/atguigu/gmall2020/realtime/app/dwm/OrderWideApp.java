package com.atguigu.gmall2020.realtime.app.dwm;

import com.alibaba.fastjson.JSON;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall2020.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall2020.realtime.bean.OrderDetail;
import com.atguigu.gmall2020.realtime.bean.OrderInfo;
import com.atguigu.gmall2020.realtime.bean.OrderWide;
import com.atguigu.gmall2020.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        String groupId = "order_wide_group";

        String orderInfoSourceTopic = "DWD_ORDER_INFO";
        String orderDetailSourceTopic = "DWD_ORDER_DETAIL";

        String orderWideSinkTopic = "DWM_ORDER_WIDE";

        FlinkKafkaConsumer<String> sourceOrderInfo = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> sourceOrderDetail = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);

        DataStream<String> orderInfojsonDstream = env.addSource(sourceOrderInfo);
        DataStream<String> orderDetailJsonDstream = env.addSource(sourceOrderDetail);


        DataStream<OrderInfo> orderInfoDStream = orderInfojsonDstream.map(new RichMapFunction<String, OrderInfo>() {
            SimpleDateFormat simpleDateFormat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public OrderInfo map(String jsonString) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(jsonString, OrderInfo.class);
                orderInfo.setCreate_ts(simpleDateFormat.parse(orderInfo.getCreate_time()).getTime());
                return orderInfo;
            }
        });
        DataStream<OrderDetail> orderDetailDstream = orderDetailJsonDstream.map(new RichMapFunction<String, OrderDetail>() {
            SimpleDateFormat simpleDateFormat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public OrderDetail map(String jsonString) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(jsonString, OrderDetail.class);
                orderDetail.setCreate_ts(simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

        //   orderDetailDstream.print();

        //设定事件时间水位
        SingleOutputStreamOperator<OrderInfo> orderInfoWithEventTimeDstream = orderInfoDStream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                return orderInfo.getCreate_ts();
            }
        }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWithEventTimeDstream = orderDetailDstream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {

            @Override
            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                return orderDetail.getCreate_ts();
            }
        }));

        //设定关联的key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDstream = orderInfoWithEventTimeDstream.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedStream = orderDetailWithEventTimeDstream.keyBy(OrderDetail::getOrder_id);

        SingleOutputStreamOperator<OrderWide> orderWideDstream = orderInfoKeyedDstream.intervalJoin(orderDetailKeyedStream).between(Time.seconds(-5), Time.seconds(5)).process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
            @Override
            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                out.collect(new OrderWide(orderInfo, orderDetail));
            }
        });

        //  orderWideDstream.print("joined ::");

        SingleOutputStreamOperator<OrderWide> orderWideWithUserDstream = AsyncDataStream.unorderedWait(orderWideDstream, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                SimpleDateFormat formattor = new SimpleDateFormat("yyyy-MM-dd");
                String birthday = jsonObject.getString("BIRTHDAY");
                Date date = formattor.parse(birthday);

                Long curTs = System.currentTimeMillis();
                Long betweenMs = curTs - date.getTime();
                Long ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L;
                Integer age = ageLong.intValue();
                orderWide.setUser_age(age);
                orderWide.setUser_gender(jsonObject.getString("GENDER"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getUser_id());
            }
        }, 10, TimeUnit.SECONDS);

        //     orderWideWithUserDstream.print("dim join user:");


        //查询省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDstream = AsyncDataStream.unorderedWait(orderWideWithUserDstream, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                orderWide.setProvince_name(jsonObject.getString("NAME"));
                orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                orderWide.setProvince_area_code(jsonObject.getString("AREA_CODE"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getProvince_id());
            }
        }, 10, TimeUnit.SECONDS);

        //查询sku维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDstream = AsyncDataStream.unorderedWait(orderWideWithProvinceDstream, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                orderWide.setTm_id(jsonObject.getLong("TM_ID"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getSku_id());
            }
        }, 10, TimeUnit.SECONDS);


        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDstream = AsyncDataStream.unorderedWait(orderWideWithSkuDstream, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getSpu_id());
            }
        }, 10, TimeUnit.SECONDS);


        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3Dstream = AsyncDataStream.unorderedWait(orderWideWithSpuDstream, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                orderWide.setCategory3_name(jsonObject.getString("NAME"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getCategory3_id());
            }
        }, 10, TimeUnit.SECONDS);


        SingleOutputStreamOperator<OrderWide> orderWideWithTmDstream = AsyncDataStream.unorderedWait(orderWideWithCategory3Dstream, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                orderWide.setTm_name(jsonObject.getString("TM_NAME"));
            }

            @Override
            public String getKey(OrderWide orderWide) {
                return String.valueOf(orderWide.getTm_id());
            }
        }, 10, TimeUnit.SECONDS);


        orderWideWithTmDstream.print("all:::");

        orderWideWithTmDstream.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
