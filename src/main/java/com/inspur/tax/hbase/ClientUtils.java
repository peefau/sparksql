package com.inspur.tax.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @program: spark_sql_test
 * @description: HBaseClient工具类，包括kerberos登录，设置配置文件等
 * @author: lipf
 * @create: 2019-03-08 21:05
 **/
public class ClientUtils {
    // for local mode
    // for spark-submit
    private static final Properties krb_props = getConfigInfo("kerberos.properties");
    private static final Properties hbase_props = getConfigInfo("hbase.properties");
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyymmdd");
    /**
    * @Description: 读取外部绝对路径的配置文件
    * @Params:  * @param propPath
    * @Return: java.util.Properties
    * @Author: lipf
    * @Date: 2019/3/22
    */
    public static Properties getConfigInfo(String propPath) {
        Properties prop = new Properties();
        try {
            InputStream in = new FileInputStream(propPath);
            try {
                prop.load(in);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return prop;
    }
    /**
     * @Description: 初始化HBase配置信息
     * @Params: * @param
     * @Return: org.apache.hadoop.conf.Configuration
     * @Author: lipf
     * @Date: 2019/3/8
     * *Date: 2019/3/12 整合进项目，修改配置文件加载及参数获取方式 宫
     */
    public static Configuration initConfig() {
        Configuration conf = null;
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum",
                    hbase_props.getProperty("hbase.zookeeper.quorum"));
            conf.set("zookeeper.znode.parent", hbase_props.getProperty("zookeeper.znode.parent"));
            conf.set("hbase.zookeeper.property.clientport", hbase_props.getProperty("hbase.zookeeper.property.clientport"));
            conf.set("hadoop.security.authentication", hbase_props.getProperty("hadoop.security.authentication"));
            conf.set("hadoop.security.authorization", hbase_props.getProperty("hadoop.security.authorization"));
            conf.set("hbase.security.authentication", hbase_props.getProperty("hbase.security.authentication"));
            conf.set("hbase.security.authorization", hbase_props.getProperty("hbase.security.authorization"));
            //HMaster地址
            conf.set("hbase.master.kerberos.principal", hbase_props.getProperty("hbase.master.kerberos.principal"));
            conf.set("hbase.regionserver.kerberos.principal", hbase_props.getProperty("hbase.regionserver.kerberos.principal"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conf;
    }

    /**
     * @Description: 初始化Kerberos访问
     * @Params: * @param
     * @Return: void
     * @Author: lipf
     * @Date: 2019/3/8
     * Date: 2019/3/12 整合进项目，修改配置文件加载及参数获取方式 宫
     */
    public static Configuration initKerberos(Configuration conf) {
        System.out.println("conf="+conf);
        try {
            System.setProperty("java.security.krb5.conf", krb_props.getProperty("keb5.file.path"));
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(krb_props.getProperty("user.principal"), krb_props.getProperty("keytab.file.path"));
            System.out.println("HBase Kerberos验证通过");
        } catch (IOException e) {
            System.out.println("HBase Kerberos验证失败");
            e.printStackTrace();
            System.out.println(krb_props.getProperty("keb5.file.path"));
            System.out.println(krb_props.getProperty("user.principal"));
            System.out.println(krb_props.getProperty("keytab.file.path"));
        }
        return conf;
    }

    /**
     * @Description: 获取HBase连接
     * @Params: * @param conf
     * @Return: org.apache.hadoop.hbase.client.Connection
     * @Author: lipf
     * @Date: 2019/3/8
     */
    public static Connection getConnection() {
        Configuration conf = initConfig();
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(initKerberos(conf));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 根据系统时间获取当前的实际数据表名
     *
     * @return
     */

    public static String getCurrentTable() {
        return hbase_props.getProperty("log.table.prefix") + sdf.format(new Date());
    }
}
