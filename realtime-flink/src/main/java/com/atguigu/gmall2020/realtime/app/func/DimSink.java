package com.atguigu.gmall2020.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall2020.realtime.common.GmallConfig;

import com.atguigu.gmall2020.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

/**
 * 没有选用异步批量写入 基于：
 * 1   这里只针对维度数据的新增变化，所以数据写入不会非常高频。
 * 2   异步写入会有一定的延迟，会对后面的join时机有影响
 * 3   异步处理需要维护多个statement 程序比较复杂
 */
public class DimSink extends RichSinkFunction<JSONObject> {

    Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 生成语句提交hbase
     *
     * @param jsonObject
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {

        String tableName = jsonObject.getString("sink_table");
        JSONObject dataJsonObj = jsonObject.getJSONObject("data");
        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            String upsertSql = genUpsertSql(tableName.toUpperCase(), jsonObject.getJSONObject("data"));
            try {
                System.out.println(upsertSql);
                Statement stat = connection.createStatement();
                stat.executeUpdate(upsertSql);
                connection.commit();
                stat.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("执行sql失败！");
            }
        }
        if (jsonObject.getString("type").equals("update") || jsonObject.getString("type").equals("delete")) {
            DimUtil.deleteCached(tableName, dataJsonObj.getString("id"));
        }
    }

    public String genUpsertSql(String tableName, JSONObject jsonObject) {
        Set<String> fields = jsonObject.keySet();
        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" + StringUtils.join(fields, ",") + ")";
        String valuesSql = " values ('" + StringUtils.join(jsonObject.values(), "','") + "')";
        return upsertSql + valuesSql;
    }

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            //  System.out.println("upsert into gmall2020.DIM_ACTIVITY_INFO(activity_name,start_time,create_time,activity_type,activity_desc,end_time,id) values ('联想专场','2020-10-22 07:49:12','','3101','联想满减','2020-11-01 07:49:15','1')");
            Statement stat = connection.createStatement();
            stat.executeUpdate("upsert into GMALL2020.DIM_ACTIVITY_INFO(activity_name,start_time,create_time,activity_type,activity_desc,end_time,id) values ('联想专场','2020-10-22 07:49:12','','3101','联想满减','2020-11-01 07:49:15','2')");
            // stat.execute("upsert into GMALL2020.DIM_ACTIVITY_INFO(activity_name,start_time,create_time,activity_type,activity_desc,end_time,id) values ('联想专场','2020-10-22 07:49:12','','3101','联想满减','2020-11-01 07:49:15','2')");
            ResultSet resultSet = stat.executeQuery("select * from  GMALL2020.DIM_ACTIVITY_INFO");
            while (resultSet.next()) {
                System.out.println("rs:" + resultSet.getString("ID"));
            }

            connection.commit();
            stat.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}