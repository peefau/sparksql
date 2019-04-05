package com.inspur.tax.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @program: spark_sql_test
 * @description: HBase操作工具类
 * @author: lipf
 * @create: 2019-03-08 21:35
 **/
@SuppressWarnings(value={"unchecked","deprecation"})
public class HBaseUtils {
    /**
     * HBase数据库连接
     */
    private static Connection conn;
    private static Admin admin;
    /**
    * @Description: 获取HBase数据库连接
    * @Params:  * @param 
    * @Return: org.apache.hadoop.hbase.client.Connection
    * @Author: lipf
    * @Date: 2019/3/11
    */ 
    public static Connection getConn() {
        return conn;
    }
    /**
    * @Description: 设置HBase数据库连接
    * @Params:  * @param conn
    * @Return: void
    * @Author: lipf
    * @Date: 2019/3/11
    */ 
    public static void setConn(Connection conn) {
        HBaseUtils.conn = conn;
    }
    /**
    * @Description: 关闭连接
    * @Params:  * @param 
    * @Return: void
    * @Author: lipf
    * @Date: 2019/3/11
    */ 
    public static void close() {
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
    * @Description: 获取库中所有表
    * @Params:  * @param connection
    * @Return: void
    * @Author: lipf
    * @Date: 2019/3/8
    */ 
    public static void listTables(){
        try {
            TableName[] tbls = conn.getAdmin().listTableNames();
            for(TableName tableName : tbls) {
                System.err.println(tableName.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
    * @Description: 读取表数据
    * @Params:  * @param tname 表名
    * @Return: void
    * @Author: lipf
    * @Date: 2019/3/9
    */
    public static void readTable(String tname){
        try {
            TableName tableName = TableName.valueOf(tname);
            Admin admin = conn.getAdmin();
            //判断当前表是否已存在，存在返回true，否则返回false
            Boolean isExists = admin.tableExists(tableName);
            if(!isExists){
                System.out.println("Table " + tableName + "is NOT EXISTS!");
                return;
            }
            
            //判断当前表是否被禁用了，是就开启
            if(admin.isTableDisabled(tableName)) {
                admin.enableTable(tableName);
            }
            
            Table table = conn.getTable(tableName);
            ResultScanner resultScanner = table.getScanner(new Scan());
            
            for(Result result : resultScanner) {
                for(Cell cell : result.listCells()) {
                    //取行键
                    String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                    //取时间戳
                    long timestamp = cell.getTimestamp();
                    //取到列族
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    //取到修饰名
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    //取到值
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println("[ rowKey = " + rowKey
                                    + ", Timestamp = " + timestamp
                                    + ", family = " + family
                                    + ", qualifier = " + qualifier
                                    + ", value = " + value + " ]"
                    );
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
    * @Description: 指定表名和列族名，创建HBase表
    * @Params:  * @param tableName 表名
    * @param cols 列族名
    * @Return: void
    * @Author: lipf
    * @Date: 2019/3/11
    */
    public static void createTable(String tableName,String[] cols) throws IOException {
        TableName tbl = TableName.valueOf(tableName);
        admin = conn.getAdmin();
        
        if(admin.tableExists(tbl)){
            System.out.println(tableName + "表已经存在！");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }
}
