package com.atguigu.gmall2020.realtime.app.dws;

import com.atguigu.gmall2020.realtime.app.UDF.KeywordProductC2RUDTF;
import com.atguigu.gmall2020.realtime.app.UDF.KeywordUDTF;
import com.atguigu.gmall2020.realtime.bean.ProvinceStats;
import com.atguigu.gmall2020.realtime.utils.ClickhouseUtil;
import com.atguigu.gmall2020.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {

    public static void main(String[] args) {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

    EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    String groupId = "province_stats";

    String orderWideTopic ="DWM_ORDER_WIDE";

        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                "province_name STRING,province_area_code STRING" +
                ",province_iso_code STRING,province_3166_2_code STRING,   order_id , " +
                "split_total_amount,    rowtime  AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR  rowtime  AS rowtime)" +
                " WITH ("+ MyKafkaUtil.getKafkaDDL(orderWideTopic,groupId)+")");


        Table provinceStateTable = tableEnv.sqlQuery("select province_id,province_name,province_area_code," +
                "province_iso_code,province_3166_2_code" +
                "sum(distinct order_id) order_ct, sum(split_total_amount) " +
                " from  ORDER_WIDE group by  TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");

        DataStream<ProvinceStats> provinceStatsDataStream =
                tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);

        provinceStatsDataStream.addSink(ClickhouseUtil.
                <ProvinceStats>getJdbcSink("insert into  province_stats  values(?,?,?,?,?,?,?,?,?,?)"));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}