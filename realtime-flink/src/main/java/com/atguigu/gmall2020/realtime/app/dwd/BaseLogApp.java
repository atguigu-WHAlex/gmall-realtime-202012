package com.atguigu.gmall2020.realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.gmall2020.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BaseLogApp {

    private static final String OPT_TYPE_START = "start";
    public static final String OPT_TYPE_PAGE = "page";
    private static final String OPT_TYPE_DISPLAY = "display";

    private static final String TOPIC_START = "DWD_START_LOG";
    private static final String TOPIC_PAGE = "DWD_PAGE_LOG";
    private static final String TOPIC_DISPLAY = "DWD_DISPLAY_LOG";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint/BaseLogApp"));

        String groupId = "ods_base_log_app";
        String topic = "ODS_BASE_LOG";
        FlinkKafkaConsumer<String> source = MyKafkaUtil.getKafkaSource(topic, groupId);

        //初始化流
        DataStreamSource<String> jsonStream = env.addSource(source);

        //字符转换成json对象
        DataStream<JSONObject> jsonObjStream = jsonStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonString, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    // 特殊字符的转码
                    String decodeJson = URLDecoder.decode(jsonString, "UTF-8");
                    JSONObject jsonObject = JSON.parseObject(decodeJson);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        final OutputTag<String> startTag = new OutputTag<String>(OPT_TYPE_START) {
        };
        final OutputTag<String> displayTag = new OutputTag<String>(OPT_TYPE_DISPLAY) {
        };


        KeyedStream<JSONObject, String> midStream = jsonObjStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> midWithNewFlagDstream = midStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            ValueState<String> firstVisitDateState = null;

            SimpleDateFormat simpleDateFormat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                if (firstVisitDateState == null) {
                    ValueStateDescriptor<String> newMidDateStateDescriptor = new ValueStateDescriptor<>("newMidDateState", String.class);
                    firstVisitDateState = getRuntimeContext().getState(newMidDateStateDescriptor);
                }
                simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                System.out.println(jsonObject);
                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                if ("1".equals(is_new)) {
                    String newMidDate = firstVisitDateState.value();
                    jsonObject.getString("ts");
                    String tsDate = simpleDateFormat.format(new Date(ts));
                    if (newMidDate != null && newMidDate.length() == 0 && !newMidDate.equals(tsDate)) {
                        //如果状态中的日期不为空 且不是操作当日， 说明设备已有历史，则设标志is_new为0
                        is_new = "0";
                        jsonObject.getJSONObject("common").put("is_new", is_new);
                    }
                    if (is_new.equals("1") && (newMidDate == null || newMidDate.length() == 0)) {
                        //经过复检之后仍旧为1 则更新状态并且把今天设为访问时间戳
                        firstVisitDateState.update(tsDate);
                    }
                }
                return jsonObject;
            }
        }).uid("check_is_new");

        SingleOutputStreamOperator<String> pageDStream = midWithNewFlagDstream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject data, Context ctx, Collector<String> out) throws Exception {
                JSONObject startJsonObj = data.getJSONObject("start");
                String dataString = data.toJSONString();
                if (startJsonObj != null && startJsonObj.size() > 0) {
                    ctx.output(startTag, dataString);
                } else {
                    System.out.println("pageString:" + dataString);
                    out.collect(dataString);
                    if (data.getJSONArray("display") != null && data.getJSONArray("display").size() > 0) {
                        JSONArray displayArr = data.getJSONArray("display");
                        for (int i = 0; i < displayArr.size(); i++) {
                            JSONObject displayJsonObj = displayArr.getJSONObject(i);
                            String pageId = data.getJSONObject("page").getString("page_id");
                            displayJsonObj.put("page_id", pageId);
                            ctx.output(displayTag, dataString);
                        }
                    }
                }
            }
        });

        DataStream<String> startDstream = pageDStream.getSideOutput(startTag);
        DataStream<String> displayDstream = pageDStream.getSideOutput(displayTag);


        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);

        startDstream.addSink(startSink);
        pageDStream.addSink(pageSink);
        displayDstream.addSink(displaySink);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();

        }

    }


}