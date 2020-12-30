package com.atguigu.gmall2020.realtime.bean;

import lombok.Data;

@Data
public class TableProcess  {

    public static final String SINK_TYPE_HBASE="HBASE";
    public static final String SINK_TYPE_KAFKA="KAFKA";
    public static final String SINK_TYPE_CK="CLICKHOUSE";

    String sourceTable;

    String operateType;

    String sinkType;

    String sinkTable;

    String sinkColumns;

    String  sinkPk;

    String sinkExtend;


}
