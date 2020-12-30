package com.atguigu.gmall2020.realtime.utils;

import com.clearspring.analytics.util.Lists;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DateTimeUtil {

    public final static DateTimeFormatter formator=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");



    public  static String  toYMDhms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formator.format(localDateTime);
    }

    public static   Long toTs(String YmDHms){

            LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formator);
            long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
            return ts;
    }


    public static void main(String[] args) {
        //testParseThreadUnSafe();
       // testParseThreadSafe();
    }

       //线程不安全测试

        public static  void testParseThreadUnSafe() {
            ExecutorService executorService = Executors.newCachedThreadPool();
            List<String> dateStrList = Lists.newArrayList(Arrays.asList(
                    "2018-04-01 10:00:01",
                    "2018-04-02 11:00:02",
                    "2018-04-03 12:00:03",
                    "2018-04-04 13:00:04",
                    "2018-04-05 14:00:05"
                    )
            );
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (String str : dateStrList) {

                executorService.execute(() -> {
                    try {
                        //创建新的SimpleDateFormat对象用于日期-时间的计算

                        Date date = simpleDateFormat.parse(str);
                        System.out.println(date.getTime());
                        TimeUnit.SECONDS.sleep(1);

                      //  simpleDateFormat = null; //销毁对象
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }

    //Java8  线程不安全测试  ？  依旧不安全啊

    @Test
    public   void  testParseThreadSafe(){

        ExecutorService executorService = Executors.newCachedThreadPool();
        List<String> dateStrList = Lists.newArrayList(Arrays.asList(
                "2018-04-01 10:00:01",
                "2018-04-02 11:00:02",
                "2018-04-03 12:00:03",
                "2018-04-04 13:00:04",
                "2018-04-05 14:00:05"
                )
        );
        DateTimeFormatter formator=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        for (String str : dateStrList) {

           executorService.execute(() -> {
                try {
                    //创建新的SimpleDateFormat对象用于日期-时间的计算
                    System.out.println(11111111);
                    LocalDateTime localDateTime = LocalDateTime.parse(str, formator);
                    System.out.println(22222222);
                    Long ts =   localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();

                    System.out.println(ts);
                    TimeUnit.SECONDS.sleep(1);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        while (true){

        }
    }


    @Test
    public   void  testPro(){
        long start = System.currentTimeMillis();
        System.out.println(start);

        List<String> dateStrList = Lists.newArrayList(Arrays.asList(
                "2018-04-01 10:00:01",
                "2018-04-02 11:00:02",
                "2018-04-03 12:00:03",
                "2018-04-04 13:00:04",
                "2018-04-05 14:00:05"
                )
        );
        DateTimeFormatter formator=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        for (int i = 0; i < 1000; i++) {
            for (String str : dateStrList) {

                try {
                    //创建新的SimpleDateFormat对象用于日期-时间的计算
                    LocalDateTime localDateTime = LocalDateTime.parse(str, formator);
                    Long ts =   localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        long end = System.currentTimeMillis();
        System.out.println(end-start);

    }

    @Test
    public   void  testPro2(){
        long start = System.currentTimeMillis();
        System.out.println(start);

        List<String> dateStrList = Lists.newArrayList(Arrays.asList(
                "2018-04-01 10:00:01",
                "2018-04-02 11:00:02",
                "2018-04-03 12:00:03",
                "2018-04-04 13:00:04",
                "2018-04-05 14:00:05"
                )
        );
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (int i = 0; i < 1000; i++) {
            for (String str : dateStrList) {

                try {
                    //创建新的SimpleDateFormat对象用于日期-时间的计算
                    Date date = simpleDateFormat.parse(str);
                    Long ts =   date.getTime();
                    //System.out.println(ts);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        long end = System.currentTimeMillis();
        System.out.println(end-start);

    }

}
