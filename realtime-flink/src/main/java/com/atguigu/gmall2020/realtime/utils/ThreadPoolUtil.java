package com.atguigu.gmall2020.realtime.utils;



import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    public static ThreadPoolExecutor pool;


    // 获取单例的线程池对象
    public static ThreadPoolExecutor getInstance() {
        if (pool == null) {
            synchronized (ThreadPoolUtil.class) {
                if (pool == null) {
                    System.out.println ("开辟程池！！！！！");
                    pool=new ThreadPoolExecutor(4, 6, 20000, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return pool;
    }

}