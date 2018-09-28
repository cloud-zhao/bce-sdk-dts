package com.bce.dts.consumer;

/**
 * region context infomation
 * 
 * @author yushaozai@baidu.com
 * @data 2017-01-04
 * @version 1.0
 */
public class RegionContext {
    private boolean usePublicIp;
    private String accessKey;
    private String secretKey;

    /**
     * get access key
     * 
     * @return    access key
     */
    public String getAccessKey() {
        return this.accessKey;
    }
    /**
     * set access key
     * 
     * @param accessKey
     */
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }
    /**
     * get secret key
     * 
     * @return    secret key
     */
    public String getSecretKey() {
        return this.secretKey;
    }
    /**
     * set secret key
     * @param secret
     */
    public void setSecretKey(String secret) {
        this.secretKey = secret;
    }

    /**
     * getter isUsePublicIp
     * 
     * @return    isUsePublicIp
     */
    public boolean isUsePublicIp() {
        return this.usePublicIp;
    }

    /**
     * setter isUsePublicIp
     * 
     * @param usePublicIp
     */
    public void setUsePublicIp(boolean usePublicIp) {
        this.usePublicIp = usePublicIp;
    }
}