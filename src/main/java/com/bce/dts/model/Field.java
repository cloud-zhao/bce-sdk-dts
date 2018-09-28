package com.bce.dts.model;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.google.protobuf.ByteString;

/**
 * Field Class
 * 
 * @author yushaozai@baidu.com
 * @date 2017年9月6日
 * @version 1.0
 */
public class Field {
    public long length;
    public boolean primaryKey;
    public String name;
    public int type;
    public String encoding;
    public ByteString value;
    public boolean changeValue = true;
    
    public String strType;
    public String valueBefore;
    public String valueAfter;

    public static Type[] MYSQL_TYPES = new Type[256];

    public Field() {
        MYSQL_TYPES[0] = Type.DECIMAL;
        MYSQL_TYPES[1] = Type.INT8;
        MYSQL_TYPES[2] = Type.INT16;
        MYSQL_TYPES[3] = Type.INT32;
        MYSQL_TYPES[4] = Type.FLOAT;
        MYSQL_TYPES[5] = Type.DOUBLE;
        MYSQL_TYPES[6] = Type.NULL;
        MYSQL_TYPES[7] = Type.TIMESTAMP;
        MYSQL_TYPES[8] = Type.INT64;
        MYSQL_TYPES[9] = Type.INT24;
        MYSQL_TYPES[10] = Type.DATE;
        MYSQL_TYPES[11] = Type.TIME;
        MYSQL_TYPES[12] = Type.DATETIME;
        MYSQL_TYPES[13] = Type.YEAR;
        MYSQL_TYPES[14] = Type.DATETIME;
        MYSQL_TYPES[15] = Type.STRING;
        MYSQL_TYPES[16] = Type.BIT;

        MYSQL_TYPES['ÿ'] = Type.GEOMETRY;
        MYSQL_TYPES['þ'] = Type.STRING;
        MYSQL_TYPES['ý'] = Type.STRING;
        MYSQL_TYPES['ü'] = Type.BLOB;
        MYSQL_TYPES['û'] = Type.BLOB;
        MYSQL_TYPES['ú'] = Type.BLOB;
        MYSQL_TYPES['ù'] = Type.BLOB;
        MYSQL_TYPES['ø'] = Type.SET;
        MYSQL_TYPES['÷'] = Type.ENUM;
        MYSQL_TYPES['ö'] = Type.DECIMAL;

        this.name = null;
        this.type = 17;
        this.length = 0L;
        this.value = null;
        this.primaryKey = false;
    }

    public Field(String name, int type, String encoding, ByteString value, boolean pk) {
        MYSQL_TYPES[0] = Type.DECIMAL;
        MYSQL_TYPES[1] = Type.INT8;
        MYSQL_TYPES[2] = Type.INT16;
        MYSQL_TYPES[3] = Type.INT32;
        MYSQL_TYPES[4] = Type.FLOAT;
        MYSQL_TYPES[5] = Type.DOUBLE;
        MYSQL_TYPES[6] = Type.NULL;
        MYSQL_TYPES[7] = Type.TIMESTAMP;
        MYSQL_TYPES[8] = Type.INT64;
        MYSQL_TYPES[9] = Type.INT24;
        MYSQL_TYPES[10] = Type.DATE;
        MYSQL_TYPES[11] = Type.TIME;
        MYSQL_TYPES[12] = Type.DATETIME;
        MYSQL_TYPES[13] = Type.YEAR;
        MYSQL_TYPES[14] = Type.DATETIME;
        MYSQL_TYPES[15] = Type.STRING;
        MYSQL_TYPES[16] = Type.BIT;

        MYSQL_TYPES['ÿ'] = Type.GEOMETRY;
        MYSQL_TYPES['þ'] = Type.STRING;
        MYSQL_TYPES['ý'] = Type.STRING;
        MYSQL_TYPES['ü'] = Type.BLOB;
        MYSQL_TYPES['û'] = Type.BLOB;
        MYSQL_TYPES['ú'] = Type.BLOB;
        MYSQL_TYPES['ù'] = Type.BLOB;
        MYSQL_TYPES['ø'] = Type.SET;
        MYSQL_TYPES['÷'] = Type.ENUM;
        MYSQL_TYPES['ö'] = Type.DECIMAL;

        this.name = name;
        this.type = type;
        this.encoding = encoding;
        if ((getType() == Type.STRING) && (this.encoding.isEmpty())) {
            this.encoding = "binary";
        }
        this.value = value;
        if (value == null) {
            this.length = -1L;
        } else {
            this.length = 0; // value.getBytes().length;
        }
        this.primaryKey = pk;
    }

    public final boolean isPrimary() {
        return this.primaryKey;
    }

    public void setPrimary(boolean primary) {
        this.primaryKey = primary;
    }

    public final String getFieldname() {
        return this.name;
    }

    public final String getEncoding() {
        if (this.encoding.equalsIgnoreCase("utf8mb4")) {
            return "utf8";
        }
        return this.encoding;
    }

    public final Type getType() {
        if ((this.type > 16) && (this.type < 246)) {
            return Type.UNKOWN;
        }
        return MYSQL_TYPES[this.type];
    }

    public boolean isChangeValue() {
        return this.changeValue;
    }

    public final ByteString getValue() {
        return this.value;
    }

    @SuppressWarnings("deprecation")
    public void mergeFrom(DataInputStream reader, String recordEncoding) throws IOException, EOFException {
        this.name = reader.readLine();
        if (this.name.isEmpty()) {
            clear();
            return;
        }

        this.type = Integer.parseInt(reader.readLine());

        this.length = Long.parseLong(reader.readLine());

        this.encoding = recordEncoding;

        if (this.length != -1L) {
            byte[] valueBytes = new byte[(int) this.length];
            reader.readFully(valueBytes);
            this.value = null;
        } else {
            this.value = null;
        }

        if (reader.readByte() == 13) {
            reader.readByte();
        }
    }

    public void clear() {
        this.type = 17;
        this.name = null;
        this.length = 0L;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(new StringBuilder().append("Field name: ").append(this.name)
                .append(System.getProperty("line.separator")).toString());
//        builder.append(new StringBuilder().append("Field type: ").append(this.type)
//                .append(System.getProperty("line.separator")).toString());
        builder.append(new StringBuilder().append("Field type: ").append(this.strType)
                .append(System.getProperty("line.separator")).toString());
        builder.append(new StringBuilder().append("Field length: ").append(this.length)
                .append(System.getProperty("line.separator")).toString());
        builder.append(new StringBuilder().append("Field value before: ").append(this.valueBefore)
                .append(System.getProperty("line.separator")).toString());
        builder.append(new StringBuilder().append("Field value after: ").append(this.valueAfter)
                .append(System.getProperty("line.separator")).toString());
        try {
            if (this.value != null) {
                if (this.encoding.equalsIgnoreCase("binary")) {
                    builder.append(new StringBuilder().append("Field value(binary): ").append("")
                            .append(System.getProperty("line.separator")).toString());
                } else {
                    builder.append(new StringBuilder().append("Field value: ")
                            .append(this.value.toString(this.encoding))
                            .append(System.getProperty("line.separator")).toString());
                }
            } else {
                builder.append(new StringBuilder().append("Field value: null")
                        .append(System.getProperty("line.separator")).toString());
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            builder.append(System.getProperty("line.separator"));
        }
        return builder.toString();
    }

    public static enum Type {
        INT8, INT16, INT24, INT32, INT64, DECIMAL, FLOAT, DOUBLE, NULL, TIMESTAMP, DATE, TIME, DATETIME, YEAR,
        BIT, ENUM, SET, BLOB, GEOMETRY, STRING, UNKOWN;
    }
}
