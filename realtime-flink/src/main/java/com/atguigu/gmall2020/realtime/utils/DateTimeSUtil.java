package com.atguigu.gmall2020.realtime.utils;

import com.clearspring.analytics.util.Lists;
import org.junit.Test;

import java.text.SimpleDateFormat;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DateTimeSUtil {

    private static final ThreadLocal<SimpleDateFormat> formator = new ThreadLocal<SimpleDateFormat>() {
        /**
         * ThreadLocal没有被当前线程赋值时或当前线程刚调用remove方法后调用get方法，返回此方法值
         */
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println("调用get方法时，当前线程共享变量没有设置，调用initialValue获取默认值！"+new Object());
            System.out.println("调用get方法时， ！"+ simpleDateFormat);
            return simpleDateFormat ;
        }
    };;


    public  static String  toYMDhms(Date date){
        return formator.get().format(date);
    }

    public static  Long toTs(String YmDHms){

           // System.out.println("|"+YmDHms+"|");
        System.out.println("formator:"+formator.get());
        try {
            long time = formator.get().parse(YmDHms).getTime();

            return time;
        } catch ( Exception e) {

            e.printStackTrace();
            throw new RuntimeException("日期格式转换失败："+YmDHms);
        }
    }



    @Test
    public void testParseThreadSafe() {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        List<String> dateStrList = Lists.newArrayList(Arrays.asList(
                "2018-04-01 10:00:01",
                "2018-04-02 11:00:02",
                "2018-04-03 12:00:03",
                "2018-04-04 13:00:04",
                "2018-04-05 14:00:05"
                )
        );

        for (String str : dateStrList) {

           executorService.execute(() -> {
                try {
                    System.out.println(Thread.currentThread().getName());
                    //创建新的SimpleDateFormat对象用于日期-时间的计算
                    Long ts = DateTimeSUtil.toTs(str);

                    System.out.println(ts);
                    TimeUnit.SECONDS.sleep(1);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    @Test
    public void testParseThreadSafe2() {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        List<String> dateStrList = Lists.newArrayList(Arrays.asList(
                "2018-04-01 10:00:01",
                "2018-04-02 11:00:02",
                "2018-04-03 12:00:03",
                "2018-04-04 13:00:04",
                "2018-04-05 14:00:05"
                )
        );
        for (String str : dateStrList) {
           executorService.execute(new Mydate(str));
        }
    }

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        List<String> dateStrList = Lists.newArrayList(Arrays.asList(
                "2018-04-01 10:00:01",
                "2018-04-02 11:00:02",
                "2018-04-03 12:00:03",
                "2018-04-04 13:00:04",
                "2018-04-05 14:00:05"
                )
        );
        for (String str : dateStrList) {
            executorService.execute(new Mydate(str));
        }
    }

    public  static  class Mydate implements Runnable{
        String dateStr ;
        public Mydate(String dateStr){
            this.dateStr=dateStr;
        }
        @Override
        public void run() {
            try {
                System.out.println(Thread.currentThread().getName());
                //创建新的SimpleDateFormat对象用于日期-时间的计算

                SimpleDateFormat simpleDateFormat = DateTimeSUtil.formator.get();
                Long ts = DateTimeSUtil.toTs(dateStr);

                System.out.println(ts);
                TimeUnit.SECONDS.sleep(1);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
