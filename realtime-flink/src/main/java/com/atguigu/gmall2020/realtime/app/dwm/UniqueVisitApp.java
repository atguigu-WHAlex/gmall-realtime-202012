package com.atguigu.gmall2020.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall2020.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;


public class UniqueVisitApp {


    public static void main(String[] args) {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint/UniqueVisitApp");
//        env.setStateBackend(fsStateBackend);


        String groupId = "unique_visit_app";
        String sourceTopic = "DWD_PAGE_LOG";
        String sinkTopic = "DWM_UNIQUE_VISIT";
        FlinkKafkaConsumer<String> source  = MyKafkaUtil.getKafkaSource(sourceTopic,groupId);

        DataStreamSource<String> jsonStream = env.addSource(source);

        DataStream<JSONObject> jsonObjStream   = jsonStream.map(JSON::parseObject);

        KeyedStream<JSONObject, String> jsonObjWithMidDstream = jsonObjStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filteredJsonObjDstream = jsonObjWithMidDstream.filter(new RichFilterFunction<JSONObject>() {

            ValueState<String> lastVisitDateState = null;

            SimpleDateFormat simpleDateFormat = null;

            /*
            初始化状态
            初始化时间格式器
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                if (lastVisitDateState == null) {
                    ValueStateDescriptor<String> lastViewDateStateDescriptor = new ValueStateDescriptor<>("lastViewDateState", String.class);
                    StateTtlConfig stateTtlConfig=StateTtlConfig.newBuilder(Time.days(1) ).build();
//                             .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //默认值
//                             .setStateVisibility( NeverReturnExpired).build();//默认值
                    lastViewDateStateDescriptor.enableTimeToLive(stateTtlConfig );

                    lastVisitDateState = getRuntimeContext().getState(lastViewDateStateDescriptor);
                }
            }

            /*
                  首先检查当前页面是否有上页标识，如果有说明该次访问一定不是当日首次
            */
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                Long ts = jsonObject.getLong("ts");
                String startDate = simpleDateFormat.format(ts);
                String lastViewDate = lastVisitDateState.value();
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if(lastPageId!=null&&lastPageId.length()>0){
                    return false;
                }
                System.out.println("起始访问");
                if (lastViewDate != null && lastViewDate.length() > 0 && startDate.equals(lastViewDate)) {
                    System.out.println("已访问：lastVisit:"+lastViewDate+"|| startDate："+startDate);
                    return false;
                } else {
                    System.out.println("未访问：lastVisit:"+lastViewDate+"|| startDate："+startDate);
                    lastVisitDateState.update(startDate);
                    return true;
                }
            }
        }).uid("uvFilter");

        SingleOutputStreamOperator<String> dataJsonStringDstream = filteredJsonObjDstream.map(JSONAware::toJSONString);
        dataJsonStringDstream.print("uv");
        dataJsonStringDstream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
