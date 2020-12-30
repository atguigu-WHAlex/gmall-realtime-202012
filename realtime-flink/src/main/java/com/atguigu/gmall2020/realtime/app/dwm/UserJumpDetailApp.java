package com.atguigu.gmall2020.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.gmall2020.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {



    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String sourceTopic = "DWD_PAGE_LOG";
        String sinkTopic = "DWM_USER_JUMP_DETAIL";
        String groupId = "UserJumpDetailApp";

//         DataStream<String> dataStream = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"home\"},\"ts\":15000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"detail\"},\"ts\":30000} "
//                );


        DataStreamSource<String> dataStream = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));
        dataStream.print("in json:");
        DataStream<JSONObject> jsonObjStream = dataStream.map(jsonString -> JSON.parseObject(jsonString));


        SingleOutputStreamOperator<JSONObject> jsonObjWithEtDstream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                return jsonObject.getLong("ts");
            }
        }));

        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjWithEtDstream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));


        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("GoIn").where(new SimpleCondition<JSONObject>() {
            @Override   // 条件1 ：进入的第一个页面
            public boolean filter(JSONObject jsonObj) throws Exception {
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if ( lastPageId==null||lastPageId.length()==0) {
                    return true;
                }
                return false;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override  //条件2： 在10秒时间范围内必须有第二个页面
            public boolean filter(JSONObject jsonObj) throws Exception {
                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                System.out.println("next:"+pageId);
                if (pageId!=null&&pageId.length()>0){
                    return true;
                }
                return false;
            }
        }).within(Time.milliseconds(10000));

        PatternStream<JSONObject> patternedStream = CEP.pattern(jsonObjectStringKeyedStream, pattern);

        final OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

        SingleOutputStreamOperator<String> filteredStream=patternedStream.flatSelect(timeoutTag, new PatternFlatTimeoutFunction<JSONObject, String>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        List<JSONObject> objectList = pattern.get("GoIn");
                        for (JSONObject jsonObject : objectList) {
                            out.collect(jsonObject.toJSONString());  //这里进入out的数据都被timeoutTag标记
                        }
            }
        },
        new PatternFlatSelectFunction<JSONObject,String>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                //因为不超时的事件不提取，所以这里不写代码
            }
        });

        DataStream<String> jumpDstream = filteredStream.getSideOutput(timeoutTag);
        jumpDstream.print("jump::");

        jumpDstream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
