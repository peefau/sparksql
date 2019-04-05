package com.inspur.tax;

/**
 * @program: spark_sql_test
 * @description: 数据开放服务中心产生的日志报文
 * @author: lipf
 * @create: 2019-03-11 16:58
 **/
public class DataGramLog {
    /**
     * 请求消息主键，在消息标准化，并且判断完缓存数据，进行完安全判断以及权限校验以后，
     * 由程序生成，添加到消息中。同样作为阻塞队列的Key，唯一标识一个请求。
     */
    private String tranSeq;
    /**
     * 日志来源，生成日志的步骤ID
     */
    private String logStepId;
    /**
     * 日志步骤名称Name
     */
    private String logStepName;
    /**
     * 消息进入步骤时记录，结束时添加。
     * 格式为1970年至今的毫秒数（Unix时间戳）
     */
    private long logTimeIn;
    /**
     * 步骤返回时记录，结束时添加。
     * 格式为1970年至今的毫秒数（Unix时间戳）
     */
    private long logTimeOut;
    /**
     * 协议类型，表明本次请求的请求类型，是EJB还是webservice的，为空或者没有取到值的情况下，默认为Rest请求
     */
    private String protType;
    /**
     * 请求的服务ID
     */
    private String serviceId;
    /**
     * 请求用户名
     */
    private String userId;
    /**
     * 生成消息的客户端信息
     */
    private String clientId;
    /**
     * 日志状态码大类
     */
    private int logCode;
    /**
     * 日志状态码小类
     */
    private String logMsgCode;
    /**
     * 信息概述
     */
    private String logMsgMsg;
    /**
     * 具体信息
     */
    private String logMsgReason;
    /**
     * 生成日志的时候对应的完整的请求/相应报文信息
     */
    private String msgBody;
    
    public String getTranSeq() {
        return tranSeq;
    }
    
    public void setTranSeq(String tranSeq) {
        this.tranSeq = tranSeq;
    }
    
    public String getLogStepId() {
        return logStepId;
    }
    
    public void setLogStepId(String logStepId) {
        this.logStepId = logStepId;
    }
    
    public String getLogStepName() {
        return logStepName;
    }
    
    public void setLogStepName(String logStepName) {
        this.logStepName = logStepName;
    }
    
    public Long getLogTimeIn() {
        return logTimeIn;
    }
    
    public void setLogTimeIn(Long logTimeIn) {
        this.logTimeIn = logTimeIn;
    }
    
    public Long getLogTimeOut() {
        return logTimeOut;
    }
    
    public void setLogTimeOut(Long logTimeOut) {
        this.logTimeOut = logTimeOut;
    }
    
    public String getProtType() {
        return protType;
    }
    
    public void setProtType(String protType) {
        this.protType = protType;
    }
    
    public String getServiceId() {
        return serviceId;
    }
    
    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getClientId() {
        return clientId;
    }
    
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
    
    public int getLogCode() {
        return logCode;
    }
    
    public void setLogCode(int logCode) {
        this.logCode = logCode;
    }
    
    public String getLogMsgCode() {
        return logMsgCode;
    }
    
    public void setLogMsgCode(String logMsgCode) {
        this.logMsgCode = logMsgCode;
    }
    
    public String getLogMsgMsg() {
        return logMsgMsg;
    }
    
    public void setLogMsgMsg(String logMsgMsg) {
        this.logMsgMsg = logMsgMsg;
    }
    
    public String getLogMsgReason() {
        return logMsgReason;
    }
    
    public void setLogMsgReason(String logMsgReason) {
        this.logMsgReason = logMsgReason;
    }
    
    public String getMsgBody() {
        return msgBody;
    }
    
    public void setMsgBody(String msgBody) {
        this.msgBody = msgBody;
    }
}
