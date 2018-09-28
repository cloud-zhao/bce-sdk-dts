package com.bce.dts.consumer;

import java.util.List;

/**
 * consumer client interface
 * 
 * @author yushaozai@baidu.com
 * @data 2017-08-04
 * @version 1.0
 */
public abstract interface ConsumerClient {
    /**
     * set dtsId
     * 
     * @param dtsId    subscribe task id
     * @throws Exception
     */
    public abstract void askForDtsId(String dtsId) throws Exception;
    /**
     * setting task admin user id (only for XDB)
     * 
     * @param userId    task admin user id
     * @throws Exception
     */
    public abstract void askForUserId(String userId) throws Exception;
    /**
     * start to consume
     * 
     * @throws Exception
     */
    public abstract void start() throws Exception;
    /**
     * wait for stop to consume
     * 
     * @param milliseconds
     * @throws InterruptedException
     */
    public abstract void waitForStop(long milliseconds) throws InterruptedException;
    /**
     * stop to consume
     * 
     * @throws Exception
     */
    public abstract void stop() throws Exception;
    /**
     * add consumer listener
     * 
     * @param paramConsumerListener
     */
    public abstract void addConcurrentListener(ConsumerListener paramConsumerListener);
    /**
     * get consumer listeners
     * 
     * @return    list of consumer listeners
     */
    public abstract List<ConsumerListener> getConcurrentListeners();
    /**
     * suspend to consume
     */
    public abstract void suspend();
    /**
     * resume to consume
     */
    public abstract void resume();
}

