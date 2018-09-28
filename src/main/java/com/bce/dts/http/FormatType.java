package com.bce.dts.http;

/**
 * http content format type
 * 
 * @author yushaozai@baidu.com
 * @data 2017-01-05
 * @version 1.0
 */
public enum FormatType {
    XML, JSON, RAW;

    /**
     * convert formatter to accept
     * 
     * @param format
     * @return
     */
    public static String mapFormatToAccept(FormatType format) {
        if (XML == format) {
            return "application/xml";
        }
        if (JSON == format) {
            return "application/json";
        }
        return "application/octet-stream";
    }

    /**
     * convert accept to formatter
     * 
     * @param accept
     * @return
     */
    public static FormatType mapAcceptToFormat(String accept) {
        if ((accept.toLowerCase().equals("application/xml"))
                || (accept.toLowerCase().equals("text/xml"))) {
            return XML;
        }
        if (accept.toLowerCase().equals("application/json")) {
            return JSON;
        }
        return RAW;
    }
}