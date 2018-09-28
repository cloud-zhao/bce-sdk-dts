package com.bce.dts.endpoints;

/**
 * dts transfer system endpoint, include ip, port
 * 
 * @author yushaozai@baidu.com
 * @date 2017-08-10
 * @version 1.0
 */
public class PubserverEndpoint {
    /**
     * endpoint ip
     */
    private String strIp;
    /**
     * endpoint port
     */
    private int intPort;
    
    /**
     * getter 
     * 
     * @return      strIp
     */
    public String getStrIp() {
        return strIp;
    }
    /**
     * setter
     * 
     * @param strIp
     */
    public void setStrIp(String strIp) {
        this.strIp = strIp;
    }
    /**
     * getter
     * 
     * @return      intPort
     */
    public int getIntPort() {
        return intPort;
    }
    /**
     * setter
     * 
     * @param intPort
     */
    public void setIntPort(int intPort) {
        this.intPort = intPort;
    }
    /**
     * get endpoint string, ip:port
     */
    public String toString() {
        return this.strIp + ":" + this.intPort;
    }
}
