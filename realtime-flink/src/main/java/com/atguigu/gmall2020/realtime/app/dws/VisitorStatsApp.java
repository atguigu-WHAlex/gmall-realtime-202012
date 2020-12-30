package com.atguigu.gmall2020.realtime.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.gmall2020.realtime.bean.VisitorStats;
import com.atguigu.gmall2020.realtime.utils.ClickhouseUtil;
import com.atguigu.gmall2020.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;

/**
 * 要不要把多个明细的同样的维度统计在一起
 * <p>
 * 因为单位时间内mid的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app版本、省市区域
 * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10秒
 */
public class VisitorStatsApp {


    /***
     * 1  各个数据在维度聚合前不具备关联性 ，所以 先进行维度聚合
     * 2  进行关联  这是一个fulljoin
     * 3  可以考虑使用flinksql 完成
     * @param args
     */
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        String groupId = "visitor_stats_app";

        String uniqueVisitSourceTopic = "DWM_UNIQUE_VISIT";
        String pageViewSourceTopic = "DWD_PAGE_LOG";
        String userJumpDetailSourceTopic = "DWM_USER_JUMP_DETAIL";


        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);

        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> userJumpDStream = env.addSource(userJumpSource);

//        uniqueVisitDStream.print("uv=====>");
//        pageViewDStream.print("pv-------->");
//        userJumpDStream.print("uj--------->");
//
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

        //转换uv流
        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDstream = uniqueVisitDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    1L, 0L, 0L, 0L, 0L, jsonObj.getLong("ts"));
        });


        //转换pv流
        SingleOutputStreamOperator<VisitorStats> pageViewStatsDstream = pageViewDStream.map(json -> {
            //  System.out.println("pv:"+json);
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 1L, 0L, 0L, jsonObj.getJSONObject("page").getLong("during_time"), jsonObj.getLong("ts"));
        });
        // pageViewStatsDstream.print("pvpvpv::");

        //转换sv流
        SingleOutputStreamOperator<VisitorStats> sessionVisitDstream = pageViewDStream.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String json, Context ctx, Collector<VisitorStats> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(json);
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() == 0) {
                    //    System.out.println("sc:"+json);
                    VisitorStats visitorStats = new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 0L, 1L, 0L, 0L, jsonObj.getLong("ts"));
                    out.collect(visitorStats);
                }
            }
        });

        //  sessionVisitDstream.print("session:===>");

        //转换跳转流
        SingleOutputStreamOperator<VisitorStats> userJumpStatDstream = userJumpDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 0L, 0L, 1L, 0L, jsonObj.getLong("ts"));
        });

        DataStream<VisitorStats> unionDetailDstream = uniqueVisitStatsDstream.union(pageViewStatsDstream, sessionVisitDstream, userJumpStatDstream);

        // unionDetailDstream.print("union ------>");

        //  SingleOutputStreamOperator<MidStats> midStatsWithWatermarkDstream = unionDetailDstream.assignTimestampsAndWatermarks(WatermarkStrategy.<MidStats>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<MidStats>() {
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDstream =
                unionDetailDstream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1)).
                                withTimestampAssigner((visitorStats, ts) -> visitorStats.getTs())
                );

        visitorStatsWithWatermarkDstream.print("after union:::");


//        visitorStats-> new Tuple4<>(visitorStats.getVc()
//                ,visitorStats.getCh(),
//                visitorStats.getAr(),
//                visitorStats.getIs_new())

        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsTuple4KeyedStream =
                visitorStatsWithWatermarkDstream
                        .keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                                   @Override
                                   public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                                       return new Tuple4<>(visitorStats.getVc()
                                               , visitorStats.getCh(),
                                               visitorStats.getAr(),
                                               visitorStats.getIs_new());

                                   }
                               }
                        );


        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowStream =
                visitorStatsTuple4KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));


        SingleOutputStreamOperator<VisitorStats> visitorStatsDstream =
                windowStream.reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                        //把度量数据两两相加
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                        return stats1;
                    }
                }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, Context context,
                                        Iterable<VisitorStats> visitorStatsIn,
                                        Collector<VisitorStats> visitorStatsOut) throws Exception {
                        //补时间字段
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (VisitorStats visitorStats : visitorStatsIn) {

                            String startDate = simpleDateFormat.format(new Date(context.window().getStart()));
                            String endDate = simpleDateFormat.format(new Date(context.window().getEnd()));

                            visitorStats.setStt(startDate);
                            visitorStats.setEdt(endDate);
                            visitorStatsOut.collect(visitorStats);
                        }
                    }
                });

        // 关于开窗口 需要知道的几件事：

        //1 时间周期 用eventTime 开的10秒的窗口都是以自然时间的 秒数的10倍数 开窗口的，所以不管何时执行，任何数据都会落在固定的窗口内
        //2 一旦超越窗口范围+水位线延迟   就会触发窗口计算
        //3  full join操作 产生的是可回溯流 意味着计算结果可能会出现修改， 这就需要写入操作必须实现幂等性。  如果多流join 意味着每个表的数据到位都会产生新数据，只有最后一个到位的数据表产生的数据才是完整的join数据
        // 3.1 放弃fulljoin操作 因为fulljoin带来的是数据一定会出现大量的重复  因为ck无法保证严格的幂等性 所以会出现一定时间内数据重复
        //4  问题？ 1 数据重发  由于时间点过了水位线，所以历史数据是不会被计算的
        //          2 如果数据重复 聚合前并没有去重操作 聚合操作也不幂等所以会出现重复累加的情况。

        visitorStatsDstream.print("reduce:");

        visitorStatsDstream.addSink(ClickhouseUtil.getJdbcSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
