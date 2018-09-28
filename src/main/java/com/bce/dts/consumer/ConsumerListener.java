package com.bce.dts.consumer;

import com.bce.dts.model.DataMessage;

/**
 * consumer listener
 * 
 * @author yushaozai@baidu.com
 * @data 2017-08-04
 * @version 1.0
 */
public abstract class ConsumerListener {
    /**
     * begin time
     */
    private long beginTime;

    /**
     * notify consumer listener to handle DataMessage
     * one DataMessage includes some records
     * 
     * @param dataMsg
     * @throws Exception
     */
    public abstract void notify(DataMessage dataMsg) throws Exception;
    /**
     * no exception
     * 
     * @param paramException
     */
    public abstract void noException(Exception paramException);

    /**
     * notify consumer listener to handle message without heartbeat info
     * 
     * @param messages
     * @throws Exception
     */
    public synchronized void notifyWithoutHeartbeat(DataMessage message) throws Exception {
        if (this.beginTime == 0L) {
            this.beginTime = System.currentTimeMillis();
        }
        try {
            notify(message);
        } catch (Exception e) {
            noException(e);
        }
    }
}

