package com.bce.dts.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * Messge super class
 * 
 * @author yushaozai@baidu.com
 * @data 2017-08-10
 * @version 1.0
 */
public class Message {
    protected int intType;
    protected long id;
    protected Map<String, String> mapAttributes;

    /**
     * constructor
     */
    public Message() {
        this.mapAttributes = new HashMap<String, String>();
    }

    /**
     * getter
     * 
     * @return      id
     */
    public long getMid() {
        return this.id;
    }
    /**
     * getter
     * 
     * @return      intType
     */
    public int getType() {
        return this.intType;
    }
    /**
     * getter 
     * 
     * @param key
     * @return      get attr value of one key
     */
    public String getAttribute(String key) {
        return (String) this.mapAttributes.get(key);
    }
    /**
     * setter
     * 
     * @param id
     */
    public void setId(long id) {
        this.id = id;
    }
    /**
     * setter
     * 
     * @param type
     */
    public void setType(int type) {
        this.intType = type;
    }
    /**
     * add attribute of one key
     * 
     * @param key
     * @param value
     */
    public void addAttribute(String key, String value) {
        this.mapAttributes.put(key, value);
    }
    /**
     * add attributes
     * 
     * @param attrs
     */
    public void addAttributes(Map<String, String> attrs) {
        this.mapAttributes.putAll(attrs);
    }

    /**
     * merge from input stream
     * 
     * @param reader
     * @throws IOException
     */
    @SuppressWarnings("deprecation")
    public void mergeFrom(DataInputStream reader) throws IOException {
        String strLine;
        while (!(strLine = reader.readLine()).isEmpty()) {
            String[] kv = StringUtils.split(strLine, ':');
            if (kv.length != 2) {
                throw new IOException("Parse message attribute " + strLine + " error");
            }

            addAttribute(kv[0], kv[1]);
        }
    }
    /**
     * clear message object
     */
    public void clear() {
        this.intType = 0;
        this.id = -1L;
        this.mapAttributes.clear();
    }
    /**
     * toString
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : this.mapAttributes.entrySet()) {
            builder.append(entry.getKey() + ":" + entry.getValue());
            builder.append(System.getProperty("line.separator"));
        }
        builder.append(System.getProperty("line.separator"));
        return builder.toString();
    }
}

