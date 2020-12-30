package com.atguigu.gmall2020.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

@Slf4j
public class DimUtil {

    public static JSONObject  getDimInfo( String tableName, String id ){
        Tuple2<String, String> kv = Tuple2.of("id", id);
        return getDimInfo(   tableName,  kv);
    }



    public static JSONObject  getDimInfo(String tableName, Tuple2<String,String>... colNameAndValue ){
        try {
            //组合查询条件
            String wheresql = " where ";
            String redisKey = "";
            for (int i = 0; i < colNameAndValue.length; i++) {
                Tuple2<String, String> nameValueTuple = colNameAndValue[i];
                String fieldName = nameValueTuple.f0;
                String fieldValue = nameValueTuple.f1;
                if (i > 0) {
                    wheresql += " and ";
                    // 根据查询条件组合redis key ，
                    redisKey += "_";
                }
                wheresql += fieldName + "='" + fieldValue + "'";
                redisKey += fieldValue;
            }

            //
            JSONObject dimInfo=null;
            String dimJson = null;
            Jedis jedis = null;
            String key = "dim:" + tableName.toLowerCase() + ":" + redisKey;
            try {
                // 从连接池获得连接
                jedis = RedisUtil.getJedis();
                // 通过key查询缓存
                dimJson = jedis.get(key);
            } catch (Exception e) {
                System.out.println("缓存异常！");
                e.printStackTrace();
            }

            String sql = null;
            if (dimJson != null) {
                dimInfo = JSON.parseObject(dimJson);
            } else {
                sql = "select * from " + tableName + wheresql;
                System.out.println("查询维度sql ：" + sql);
                List<JSONObject> objectList = PhoenixUtil.queryList(sql, JSONObject.class);
                if(objectList.size()>0){
                    dimInfo = objectList.get(0);
                    if (jedis != null) {
                        //把从数据库中查询的数据同步到缓存
                        jedis.setex(key, 3600 * 24, dimInfo.toJSONString());
                    }
                }  else {
                    System.out.println("维度数据未找到：" + sql);
                }
            }
            if (jedis != null) {
                jedis.close();
                System.out.println("关闭缓存连接 ");
            }
            return dimInfo;
        }catch (Exception e){
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
    }

    public static  void deleteCached( String tableName, String id){
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }

    }

    public static JSONObject  getDimInfoNoCache(String tableName, Tuple2<String,String>... colNameAndValue ){
        try {
            //组合查询条件
            String wheresql = " where ";
            for (int i = 0; i < colNameAndValue.length; i++) {
                Tuple2<String, String> nameValueTuple = colNameAndValue[i];
                String fieldName = nameValueTuple.f0;
                String fieldValue = nameValueTuple.f1;
                if (i > 0) {
                    wheresql += " and ";

                }
                wheresql += fieldName + "='" + fieldValue + "'";
            }
            //
            JSONObject dimInfo=null;
            String dimJson = null;

            String sql = null;
            if (dimJson != null) {
                dimInfo = JSON.parseObject(dimJson);
            } else {
                sql = "select * from " + tableName + wheresql;
                System.out.println("查询维度sql ：" + sql);
                List<JSONObject> objectList = PhoenixUtil.queryList(sql, JSONObject.class);
                if(objectList.size()>0){
                    dimInfo = objectList.get(0);

                }  else {
                    System.out.println("维度数据未找到：" + sql);
                }
            }
            return dimInfo;
        }catch (Exception e){
            System.out.println(e.getMessage());
            throw new RuntimeException();
        }
    }

}
