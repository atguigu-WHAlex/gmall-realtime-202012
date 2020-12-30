package com.atguigu.gmall2020.realtime.app.func;

import com.alibaba.fastjson.JSONObject;


import com.atguigu.gmall2020.realtime.utils.DimUtil;
import com.atguigu.gmall2020.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private ExecutorService executorService = null;

    private String tableName = null;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    public void open(Configuration parameters) {
        System.out.println("获得程池！ ");
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    String key = getKey(obj);
                    JSONObject jsonObject = DimUtil.getDimInfo(tableName, key);
                    if (jsonObject != null) {
                        join(obj, jsonObject);
                    }

                    resultFuture.complete(Arrays.asList(obj));

                } catch (Exception e) {
                    System.out.println(String.format("异步查询异常. %s", e));
                    e.printStackTrace();
                }
            }
        });
    }
}