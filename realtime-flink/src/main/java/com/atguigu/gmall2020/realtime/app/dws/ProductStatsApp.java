package com.atguigu.gmall2020.realtime.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.gmall2020.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall2020.realtime.bean.OrderWide;
import com.atguigu.gmall2020.realtime.bean.PaymentWide;
import com.atguigu.gmall2020.realtime.bean.ProductStats;
import com.atguigu.gmall2020.realtime.bean.VisitorStats;
import com.atguigu.gmall2020.realtime.common.GmallConstant;
import com.atguigu.gmall2020.realtime.utils.ClickhouseUtil;
import com.atguigu.gmall2020.realtime.utils.DateTimeUtil;
import com.atguigu.gmall2020.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 目标： 形成 以商品为准的 统计  曝光 点击  购物车  下单 支付  退单  评论数
 */
public class ProductStatsApp {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String groupId = "product_stats_app";

        String pageViewSourceTopic = "DWD_PAGE_LOG";
        String orderWideSourceTopic = "DWM_ORDER_WIDE";
        String paymentWideSourceTopic = "DWM_PAYMENT_WIDE";
        String cartInfoSourceTopic = "DWD_CART_INFO";
        String favorInfoSourceTopic = "DWD_FAVOR_INFO";
        String refundInfoSourceTopic = "DWD_ORDER_REFUND_INFO";
        String commentInfoSourceTopic = "DWD_COMMENT_INFO";

        //从kafka主题中获得数据流
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //把json字符串数据流转换为统一数据对象的数据流
        SingleOutputStreamOperator<ProductStats> pageAndDispStatsDstream = pageViewDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String json, Context ctx, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(json);
                JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                String pageId = pageJsonObj.getString("page_id");
                if (pageId == null) {
                    System.out.println(jsonObj);
                }
                Long ts = jsonObj.getLong("ts");
                if ("good_detail".equals(pageId)) {
                    Long skuId = pageJsonObj.getLong("item");
                    ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1L).ts(ts).build();
                    out.collect(productStats);
                }
                JSONArray displays = jsonObj.getJSONArray("display");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if (display.getString("item_type").equals("sku_id")) {
                            Long skuId = display.getLong("item");
                            ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                            out.collect(productStats);
                        }
                    }
                }
            }
        });

        SingleOutputStreamOperator<ProductStats> orderWideStatsDstream = orderWideDStream.map(json -> {
            OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
            System.out.println("orderWide:===" + orderWide);
            String create_time = orderWide.getCreate_time();
            Long ts = DateTimeUtil.toTs(create_time);
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getSplit_total_amount()).ts(ts).build();
        });

        SingleOutputStreamOperator<ProductStats> favorStatsDstream = favorInfoDStream.map(json -> {
            JSONObject favorInfo = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(favorInfo.getString("create_time"));
            return ProductStats.builder()
                    .sku_id(favorInfo.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(ts)
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> cartStatsDstream = cartInfoDStream.map(json -> {
            JSONObject cartInfo = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(cartInfo.getString("create_time"));
            return ProductStats.builder().sku_id(cartInfo.getLong("sku_id")).cart_ct(1L).ts(ts).build();
        });

        SingleOutputStreamOperator<ProductStats> paymentStatsDstream = paymentWideDStream.map(json -> {
            PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
            Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
            return ProductStats.builder().sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                    .ts(ts).build();
        });

        SingleOutputStreamOperator<ProductStats> refundStatsDstream = refundInfoDStream.map(json -> {
            JSONObject refundJsonObj = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
            return ProductStats.builder()
                    .sku_id(refundJsonObj.getLong("sku_id"))
                    .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                    .ts(ts).build();
        });

        SingleOutputStreamOperator<ProductStats> commonInfoStatsDstream = commentInfoDStream.map(json -> {
            JSONObject commonJsonObj = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
            Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
            ProductStats productStats = ProductStats.builder().sku_id(commonJsonObj.getLong("sku_id"))
                    .comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
            return productStats;
        });

        //把统一的数据结构流合并为一个流
        DataStream<ProductStats> productStatDetailDStream = pageAndDispStatsDstream.union(
                orderWideStatsDstream, cartStatsDstream,
                paymentStatsDstream, refundStatsDstream, favorStatsDstream,
                commonInfoStatsDstream);

        productStatDetailDStream.print("after union:");
        //设定事件时间与水位线
        SingleOutputStreamOperator<ProductStats> productStatsWithTsStream =
                productStatDetailDStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ProductStats>forMonotonousTimestamps().withTimestampAssigner(
                                (productStats, recordTimestamp) -> {
                                    return productStats.getTs();
                                })
                );

        // 分组、开窗、聚合
        SingleOutputStreamOperator<ProductStats> productStatsDstream =
                productStatsWithTsStream.keyBy(new KeySelector<ProductStats, Long>() {
                    @Override
                    public Long getKey(ProductStats productStats) throws Exception {
                        return productStats.getSku_id();
                    }
                }).
                        window(TumblingEventTimeWindows.of(Time.seconds(10))).reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {

                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats1.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                        return stats1;

                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> productStatsIterable, Collector<ProductStats> out) throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (ProductStats productStats : productStatsIterable) {
                            productStats.setStt(simpleDateFormat.format(window.getStart()));
                            productStats.setEdt(simpleDateFormat.format(window.getEnd()));
                            productStats.setTs(new Date().getTime());
                            out.collect(productStats);
                        }
                    }
                });
//
//
        productStatsDstream.print("productStatsDstream::");
//
        //查询sku维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDstream =
                AsyncDataStream.unorderedWait(productStatsDstream,
                        new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                                productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                                productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                                productStats.setTm_id(jsonObject.getLong("TM_ID"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSku_id());
                            }
                        }, 10, TimeUnit.SECONDS);


        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
                AsyncDataStream.unorderedWait(productStatsWithSkuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 10, TimeUnit.SECONDS);


        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSkuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 10, TimeUnit.SECONDS);


        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 10, TimeUnit.SECONDS);
//
        productStatsWithTmDstream.print();
//
        productStatsWithTmDstream.addSink(ClickhouseUtil.<ProductStats>getJdbcSink(
                "insert into product_stats  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));
//


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
