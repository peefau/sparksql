package com.inspur.tax;

import com.inspur.tax.hbase.ClientUtils;
import com.inspur.tax.hbase.HBaseUtils;

/**
 * @program: spark_sql_test
 * @description: java远程访问HBase示例
 * @author: lipf
 * @create: 2019-03-08 21:44
 **/
public class HBaseSample {
    public static void main(String[] args) {
        try {
            HBaseUtils.setConn(ClientUtils.getConnection());
            if(HBaseUtils.getConn() == null) {
                System.out.println("HBase connection is null");
                System.exit(1);
            }
            /**
             * 先创建表
             */
            /*
            String[] cols = new String[] {"request","log","msg"};
            Date now = DateUtils.now();
            SimpleDateFormat sdf = new SimpleDateFormat(DateUtils.YMD);
            for(int i = 0; i < 3; i++) {
                String ymd = sdf.format(now);
                String tableName = "rosp_proxy_log_"+ymd;
                System.err.println("tableName = " + tableName);
                HBaseUtils.createTable(tableName,cols);
                now = DateUtils.addDay(now,i);
            }
            */
            //插入数据
            DataGramLog dataGramLog = new DataGramLog();
//            for(int i = 0; i < 100;i++) {
//                System.err.println("i = " + i);
//                dataGramLog.setTranSeq(String.valueOf(UUID.randomUUID()));
//                dataGramLog.setLogStepId("logStepId"+i);
//                dataGramLog.setLogStepName("logStepName"+i);
//                dataGramLog.setLogTimeIn((long)i);
//                dataGramLog.setLogTimeOut((long)i);
//                dataGramLog.setLogStepId("logStepId"+i);
//                dataGramLog.setLogStepName("logStepName"+i);
//                dataGramLog.setProtType("protType"+i);
//                dataGramLog.setServiceId("serviceId"+i);
//                dataGramLog.setUserId("userId"+i);
//                dataGramLog.setClientId("clientId"+i);
//                dataGramLog.setLogCode(i);
//                dataGramLog.setLogMsgCode("logMsgCode"+i);
//                dataGramLog.setLogMsgMsg("logMsgMsg"+i);
//                dataGramLog.setLogMsgReason("logMsgReason"+i);
//                dataGramLog.setMsgBody("msgBody"+i);
//
//                HBaseUtils.insertData("rosp_proxy_log_20190311",dataGramLog);
//            }
//
            //列出所有表
            HBaseUtils.listTables();
//            HBaseUtils.readTable("rosp_proxy_log_20190311");
            HBaseUtils.close();
            
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
