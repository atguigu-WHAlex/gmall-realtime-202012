package com.atguigu.gmall2020.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall2020.realtime.bean.TableProcess;
import com.atguigu.gmall2020.realtime.common.GmallConfig;
import com.atguigu.gmall2020.realtime.utils.MySQLUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    private OutputTag<JSONObject> hbaseTag = null;

    public TableProcessFunction(OutputTag<JSONObject> hbaseTag) {
        this.hbaseTag = hbaseTag;
    }

    //用于在内存中存储表配置对象
    private Map<String, TableProcess> tableProcessMap = null;

    //缓存记录目前已经存在hbase表
    private HashSet<String> existsTables = new HashSet<>();

    /**
     * 周期性更新 处理配置表
     */
    private Map<String, TableProcess> refreshMeta() {
        System.out.println("更新处理信息！");
        HashMap<String, TableProcess> tableProcessMap = new HashMap<>();
        List<TableProcess> tableProcessList = MySQLUtil.queryList("select source_table,operate_type,sink_type,sink_table,sink_columns,sink_pk,sink_extend from  table_process", TableProcess.class, true);
        for (TableProcess tableProcess : tableProcessList) {
            //用来源表名+操作类型为key
            String key = tableProcess.getSourceTable() + ":" + tableProcess.getOperateType();
            tableProcessMap.put(key, tableProcess);
            // 如果是hbase 且为插入操作时，要判断是否存在该表 没有的话要建表
            if ("insert".equals(tableProcess.getOperateType()) && "hbase".equals(tableProcess.getSinkType())) {
                //existsTables 用于缓存下来 哪些表已经存在，避免频繁尝试向hbase建表。
                boolean noExists = existsTables.add(tableProcess.getSourceTable());
                if (noExists) {
                    checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
                }
            }
        }
        if (tableProcessMap.size() == 0) {
            throw new RuntimeException("缺少处理信息");
        }
        return tableProcessMap;
    }



    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] cols = StringUtils.split(sinkColumns, ",");
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        List<String> columnList = Arrays.asList(cols);
        entries.removeIf(entry -> !columnList.contains(entry.getKey()));
    }

    /**
     * 根据配置表的信息为每条数据打标签，走kafka还是hbase
     */
    @Override
    public void processElement(JSONObject jsonObject, Context ctx, Collector<JSONObject> out) throws Exception {
        String sourceTable = jsonObject.getString("table");
        String optType = jsonObject.getString("type");
        JSONObject dataJsonObj = jsonObject.getJSONObject("data");
        if (dataJsonObj == null || dataJsonObj.size() < 3) {
            return;
        }
        //此处是maxwell批量操作标识，因为处理方式和insert无异, 所以替换成insert 操作
        if (optType.equals("bootstrap-insert")) {
            optType = "insert";
            jsonObject.put("type", "insert");
        }
        if (tableProcessMap != null && tableProcessMap.size() > 0) {
            String key = sourceTable + ":" + optType;
            TableProcess tableProcess = tableProcessMap.get(key);
            if (tableProcess != null) {
                System.out.println("tableProcess:" + tableProcess);
                jsonObject.put("sink_table", tableProcess.getSinkTable());
                //根据配置标的sinkColumns字段，过掉不需要的数据字段
                if (tableProcess.getSinkColumns() != null && tableProcess.getSinkColumns().length() > 0) {
                    System.out.println(tableProcess.getSinkColumns());
                    filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());
                }
            } else {
                System.out.println("no this key :" + key);
            }
            //根据配置表中的SINK_TYPE，来标识该条数据的流向
            if (tableProcess != null && TableProcess.SINK_TYPE_HBASE.equalsIgnoreCase(tableProcess.getSinkType())) {
                ctx.output(hbaseTag, jsonObject);
            } else if (tableProcess != null && TableProcess.SINK_TYPE_KAFKA.equalsIgnoreCase(tableProcess.getSinkType())) {
                out.collect(jsonObject);
            }
        }

    }

    public Connection connection = null;

    /**
     * 初始化 phoenix连接
     * 初始化 动态配置表
     * 开启周期调度  周期同步配置表
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        tableProcessMap = (HashMap<String, TableProcess>) refreshMeta();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                tableProcessMap = refreshMeta();
            }
        }, 5000, 5000);
    }


    /**
     * 检查 并 根据给定的字段和信息进行建表
     *
     * @param tableName
     * @param fields
     * @param pk
     * @param ext
     */
    public void checkTable(String tableName, String fields, String pk, String ext) {
        if (pk == null) {
            pk = "id";
        }
        if (ext == null) {
            ext = "";
        }
        //通过组合字符串组成建表语句 create table  if not exists schema.table(col1 varchar primary ,col2 varchar ...) salt_buckets=3;
        StringBuilder createSql = new StringBuilder("create table if not exists  " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] fieldsArr = fields.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];

            if (field.equals(pk)) {
                createSql.append(field + " varchar");
                createSql.append(" primary key ");
            } else {
                createSql.append("info." + field + " varchar");
            }
            if (i < fieldsArr.length - 1) {
                createSql.append(",");
            }

        }

        createSql.append(")");
        createSql.append(ext);
        try {
            Statement stat = connection.createStatement();
            System.out.println(createSql);
            stat.execute(createSql.toString());
            stat.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
