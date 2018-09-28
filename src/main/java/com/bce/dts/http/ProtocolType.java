package com.bce.dts.http;

/**
 * http protocol type
 * 
 * @author yushaozai@baidu.com
 * @data 2017-01-05
 * @version 1.0
 */
public enum ProtocolType {
    HTTP("http"), 
    HTTPS("https");

    private final String protocol;

    /**
     * constructor
     * 
     * @param protocol
     */
    private ProtocolType(String protocol) {
        this.protocol = protocol;
    }

    /**
     * get protocol
     * 
     * @return protocol
     */
    public String toString() {
        return this.protocol;
    }
}