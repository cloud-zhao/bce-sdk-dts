package com.bce.dts.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data Message
 * 
 * @author yushaozai@baidu.com
 * @data 2017-08-10
 * @version 1.0
 */
public class DataMessage extends Message {
    private static final Logger logger = LoggerFactory.getLogger(DataMessage.class);   

    /**
     * list of records
     */
    private final List<Record> records;

    /**
     * constructor
     */
    public DataMessage() {
        this.intType = 100;
        this.records = new ArrayList<Record>();
    }

    /**
     * getter
     * 
     * @return
     */
    public int getRecordCount() {
        return this.records.size();
    }

    /**
     * getter
     * 
     * @return
     */
    public List<Record> getRecordList() {
        return this.records;
    }

    /**
     * mergeFrom
     */
    public void mergeFrom(DataInputStream reader) throws IOException {
        while (true) {
            Record record = new Record();
            record.mergeFrom(reader);
            if (record.isEnding()) {
                break;
            }
            this.records.add(record);
        }
    }

    /**
     * mergeFromBinary
     * 
     * @param reader
     * @throws IOException
     */
    public void mergeFromBinary(DataInputStream reader) throws IOException {
        Record record = new BinlogRecord();
        record.mergeFrom(reader);
        this.records.add(record);
    }

    /**
     * clear
     */
    public void clear() {
        super.clear();
        this.records.clear();
    }

    /**
     * toString
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(super.toString());
        for (Record record : this.records) {
            builder.append(record.toString());
        }
        builder.append(System.getProperty("line.separator"));
        return builder.toString();
    }

    /**
     * add record
     * 
     * @param r
     */
    public void addRecord(Record r) {
        this.records.add(r);
    }

    /**
     * get max offset in all records
     * 
     * @return        max offset
     */
    private String getMaxOffset() {
        long maxOffset = 0;
        
        for (Record rec : this.records) {
            if (Long.valueOf(rec.getAttribute("offset")) > maxOffset) {
                maxOffset = Long.valueOf(rec.getAttribute("offset"));
            }
        }
        return String.valueOf(maxOffset);
    }
}
