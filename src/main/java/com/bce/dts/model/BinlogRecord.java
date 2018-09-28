package com.bce.dts.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * binlog record
 * 
 * @author yushaozai@baidu.com
 * @data 2017-08-10
 * @version 1.0
 */
public class BinlogRecord extends Record {
    private static final String SEP = System.getProperty("line.separator");
    private int intBrVersion = -1;
    private int intOp = -1;
    private int intLastInLogevent = -1;
    private long longSrcCategory = -1L;
    private long id = -1L;
    private long longFileNameOffset = -1L;
    private long longFileOffset = -1L;

    public BinlogRecord() {
        this.fields = new ArrayList<Field>();
    }

    public byte[] getRawData() {
        return null;
    }

    public void parse(byte[] data) throws IOException {
    }

    public int getVersion() {
        return this.intBrVersion;
    }

    public boolean isQueryBack() {
        switch ((int) this.longSrcCategory) {
            case 0:
                return false;
            case 1:
                return true;
            case 2:
                return false;
            case 3:
                return false;
            default:
                return false;
        }
    }

    public boolean isFirstInLogevent() {
        return this.intLastInLogevent == 1;
    }

    public void mergeFrom(DataInputStream is) throws IOException {
    }

    public Record.Type getOpt() {
        return Record.Type.valueOf(this.intOp);
    }

    public String getId() {
        return Long.toString(this.id);
    }

    public String getCheckpoint() {

        return new StringBuilder().append(this.longFileOffset).append("@")
                .append(this.longFileNameOffset).toString();
    }

    @Deprecated
    public String getMetadataVersion() {
        return "0";
    }

    public int getFieldCount() {
        return getFieldList().size();
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(
                new StringBuilder().append("type:").append(getOpt()).toString())
                .append(SEP);
        builder.append(new StringBuilder().append("record_id:").append(getId())
                .toString()).append(SEP);
        builder.append(new StringBuilder().append("db:").append(getDbname())
                .toString()).append(SEP);
        builder.append(new StringBuilder().append("tb:").append(getTablename())
                .toString()).append(SEP);
        builder.append(new StringBuilder().append("serverId:")
                .append(getServerId()).toString()).append(SEP);
        builder.append(new StringBuilder().append("checkpoint:")
                .append(getCheckpoint()).toString()).append(SEP);
        builder.append(new StringBuilder().append("primary_value:")
                .append(getPrimaryKeys()).toString()).append(SEP);
        builder.append(new StringBuilder().append("unique_keys:")
                .append(getUniqueColNames()).toString()).append(SEP);
        builder.append(SEP);
        getFieldList();
        for (Field field : this.fields) {
            builder.append(field.toString());
        }
        builder.append(SEP);
        return builder.toString();
    }
}
