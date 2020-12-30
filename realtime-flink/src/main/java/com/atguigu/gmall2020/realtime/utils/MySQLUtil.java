package com.atguigu.gmall2020.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall2020.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.atguigu.gmall2020.realtime.utils.MyKafkaUtil.DEFAULT_TOPIC;
import static com.atguigu.gmall2020.realtime.utils.MyKafkaUtil.kafkaServer;


public class MySQLUtil {


    /**
     * mysql查询方法，根据给定的class类型 返回对应类型的元素列表
     * @param sql
     * @param clazz
     * @param underScoreToCamel 是否把对应字段的下划线名转为驼峰名
     * @param <T>
     * @return
     */
    public static  <T> List<T> queryList(String sql,Class<T> clazz,Boolean underScoreToCamel ) {
        try {

            Class.forName("com.mysql.jdbc.Driver");
            List<T> resultList  = new ArrayList<T>();
            Connection conn   = DriverManager.getConnection("jdbc:mysql://hdp1:3306/gmall_realtime?characterEncoding=utf-8&useSSL=false","root","123123");
            Statement stat   = conn.createStatement();
            ResultSet rs   = stat.executeQuery(sql );
            ResultSetMetaData md  = rs.getMetaData();
            while (  rs.next() ) {
                T obj = clazz.newInstance();
                for (int i=1;  i<= md.getColumnCount() ;i++  ) {
                    String propertyName=md.getColumnName(i);
                    if(underScoreToCamel) {
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,md.getColumnName(i));
                    }
                    BeanUtils.setProperty(obj,propertyName, rs.getObject(i));
                }
                resultList.add(obj);
            }

            stat.close();
            conn.close();
            return  resultList ;

        } catch ( Exception e) {
            e.printStackTrace();
            throw  new RuntimeException("查询mysql失败！");
        }
    }

    /**
     * 测试验证
     * @param args
     */
    public static void main(String[] args) {
        List<TableProcess> tableProcesses = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }


}
