package com.bce.dts.consumer;

import java.net.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bce.dts.client.SubscribeClient;
import com.bce.dts.http.HttpClient;

/**
 * implments of ConsumerClient
 * 
 * @author yushaozai@baidu.com
 * @data 2017-08-04
 * @version 1.0
 */
public class ConsumerClientImpl implements ConsumerClient {
    /**
     * slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(ConsumerClientImpl.class);         
    /**
     * subscribe dts task id
     */
    private String dtsId;
    /**
     * task admin user id
     */
    private String userId;
    /**
     * consumer listeners
     */
    private List<ConsumerListener> listeners;
    /**
     * httpClient
     */
    private HttpClient httpClient;
    /**
     * Subscribe client
     */
    private SubscribeClient subClient;

    private Proxy proxy;

    public ConsumerClientImpl(RegionContext regionContext, Proxy proxy) {
        this.listeners = new ArrayList<ConsumerListener>();
        this.httpClient = new HttpClient(regionContext, proxy);
        this.proxy = proxy;
    }

    /**
     * constructor
     * 
     * @param regionContext
     */
    public ConsumerClientImpl(RegionContext regionContext) {
        this(regionContext, null);
    }
    
    /**
     * setting dtsId to be consumed
     */
    public void askForDtsId(String dtsId) throws Exception {
        this.dtsId = dtsId;
    }
    
    /**
     * setting task admin user id (only for XDB)
     */
    public void askForUserId(String userId) throws Exception {
        this.userId = userId;
    }
    
    /**
     * start to consume
     * 
     * @throws Exception
     */
    public void start() throws Exception {
        if (this.listeners.isEmpty()) {
            throw new Exception("no listeners registered");
        }
        logger.debug("start to run:{}", "DTS SDK");

        // create subscribe client thread to work
        this.subClient = new SubscribeClient(this.httpClient, this.listeners, this.dtsId, this.userId, proxy);
        this.subClient.start();
        logger.debug("proxy: " + proxy);
    }
    /**
     * wait for stop to consume
     * 
     * @param milliseconds
     * @throws InterruptedException
     */
    public void waitForStop(long milliseconds) throws InterruptedException {
    }
    /**
     * stop to consume
     * 
     * @throws Exception
     */
    public void stop() throws Exception {
        this.subClient.clientExit();
    }
    /**
     * add consumer listener
     * 
     * @param paramConsumerListener
     */
    public void addConcurrentListener(ConsumerListener listener) {
        this.listeners.add(listener);
    }
    /**
     * get consumer listeners
     * 
     * @return    list of consumer listeners
     */
    public List<ConsumerListener> getConcurrentListeners() {
        return this.listeners;
    }
    /**
     * get dtsId
     * 
     * @return    dtsId
     */
    public final String getDtsId() {
        return this.dtsId;
    }
    /**
     * suspend to consume
     */
    public void resume() {
    }
    /**
     * resume to consume
     */
    public void suspend() {
    }
}