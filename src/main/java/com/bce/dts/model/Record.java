package com.bce.dts.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * Record Class
 * 
 * @author yushaozai@baidu.com
 * @date 2017年9月6日
 * @version 1.0
 */
public class Record {
    private Type type;
    private Map<String, String> attributes;
    protected List<Field> fields;
    protected String timestamp;
    protected String txBeginTimestamp;
    protected static String gloalTxBeginTimestamp;
    protected static boolean txEnd = true;

    private boolean ending = false;

    /**
     * constructor
     */
    public Record() {
        this.ending = false;
        this.attributes = new HashMap<String, String>();
        this.fields = new ArrayList<Field>();
    }

    /**
     * is ending
     * 
     * @return
     */
    boolean isEnding() {
        return this.ending;
    }

    @SuppressWarnings("deprecation")
    public void mergeFrom(DataInputStream reader) throws IOException {
        boolean first = true;
        String line;
        while (!(line = reader.readLine()).isEmpty()) {
            String[] kv = StringUtils.split(line, ':');
            if (kv.length == 2) {
                addAttribute(kv[0], kv[1]);
                first = false;
            }
        }
        if (first == true) {
            this.ending = true;
            return;
        }

        String textPKs = getPrimaryKeys();
        List<String> pkList = Collections.emptyList();
        if ((textPKs != null) && (!textPKs.isEmpty())) {
            pkList = Arrays.asList(textPKs.split(","));
        }
        String stype = getAttribute("record_type");
        this.type = Type.valueOf(stype.toUpperCase());
        if (this.type == null) {
            this.type = Type.UNKNOWN;
        }

        this.timestamp = getAttribute("timestamp");
        if (this.type == Type.BEGIN) {
            gloalTxBeginTimestamp = this.timestamp;
            txEnd = false;
        }
        if (txEnd) {
            gloalTxBeginTimestamp = this.timestamp;
        }

        if ((this.type == Type.COMMIT) || (this.type == Type.ROLLBACK)) {
            txEnd = true;
        }
        this.txBeginTimestamp = new String(gloalTxBeginTimestamp);
        while (true) {
            Field field = new Field();
            field.mergeFrom(reader, getAttribute("record_encoding"));
            if (field.name == null) {
                break;
            }
            if ((textPKs != null) && (!textPKs.isEmpty()) && (pkList.contains(field.name))) {
                field.primaryKey = true;
            }

            this.fields.add(field);
        }

        String fieldsEncodings = getAttribute("fields_enc");

        if ((fieldsEncodings != null) && (!fieldsEncodings.isEmpty())) {
            String[] encodings = fieldsEncodings.split(",", -1);
            if (encodings.length == this.fields.size()) {
                for (int i = 0; i < encodings.length; i++) {
                    String enc = encodings[i];
                    Field field = (Field) this.fields.get(i);
                    if (enc.isEmpty()) {
                        if (field.getType() == Field.Type.STRING) {
                            field.encoding = "binary";
                        } else {
                            field.encoding = "";
                        }
                    } else {
                        if (field.getType() == Field.Type.BLOB) {
                            field.type = 15;
                        }
                        field.encoding = enc;
                    }
                }
            } else if (encodings.length * 2 == this.fields.size()) {
                for (int i = 0; i < encodings.length; i++) {
                    String enc = encodings[i];
                    Field field1 = (Field) this.fields.get(i * 2);
                    Field field2 = (Field) this.fields.get(i * 2 + 1);
                    if (enc.isEmpty()) {
                        if (field1.getType() == Field.Type.STRING) {
                            field1.encoding = "binary";
                            field2.encoding = "binary";
                        } else {
                            field1.encoding = "";
                            field2.encoding = "";
                        }
                    } else {
                        if (field1.getType() == Field.Type.BLOB) {
                            field1.type = 15;
                            field2.type = 15;
                        }
                        field1.encoding = enc;
                        field2.encoding = enc;
                    }
                }
            }
        }
    }

    /**
     * getter
     * 
     * @return
     */
    public Type getOpt() {
        return this.type;
    }

    /**
     * getter
     * 
     * @return
     */
    public String getId() {
        return getAttribute("record_id");
    }

    /**
     * getter
     * 
     * @return
     */
    public String getDbname() {
        return getAttribute("db");
    }

    /**
     * getter
     * 
     * @return
     */
    public String getTablename() {
        return getAttribute("table_name");
    }

    /**
     * getter
     * 
     * @return
     */
    public String getCheckpoint() {
        return getAttribute("checkpoint");
    }

    @Deprecated
    public String getMetadataVersion() {
        return getAttribute("meta");
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    public String getTxBeginTimestamp() {
        return this.txBeginTimestamp;
    }

    public String getServerId() {
        return getAttribute("instance");
    }

    public String getPrimaryKeys() {
        return getAttribute("primary");
    }

    public String getTraceInfo() {
        return "";
    }

    public String getUniqueColNames() {
        return getAttribute("unique");
    }

    public boolean isQueryBack() {
        String cate = getAttribute("source_category");
        if ((cate.equalsIgnoreCase("full_recorded")) || (cate.equalsIgnoreCase("part_recorded"))
                || (cate.equalsIgnoreCase("full_faked"))) {
            return false;
        }
        return true;
    }

    public boolean isFirstInLogevent() {
        String isFirstLogevent = getAttribute("logevent");
        if ((isFirstLogevent != null) && (isFirstLogevent.equals("1"))) {
            return true;
        }
        return false;
    }

    public String getAttribute(String key) {
        return (String) this.attributes.get(key);
    }

    public Map<String, String> getAttributes() {
        return this.attributes;
    }

    public int getFieldCount() {
        return this.fields.size();
    }

    public List<Field> getFieldList() {
        return this.fields;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void addAttribute(String key, String value) {
        this.attributes.put(key, value);
    }

    public byte[] getRawData() {
        return new byte[100];
    }

    public String getThreadId() throws IOException {
        return getAttribute("threadid");
    }

    public String getTraceId() throws IOException {
        return getAttribute("traceid");
    }

    public void parse(byte[] data) throws IOException {
        throw new IOException(new StringBuilder().append(Record.class.getName())
                .append(" not support parse from raw data").toString());
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : this.attributes.entrySet()) {
            builder.append(new StringBuilder().append((String) entry.getKey()).append(":")
                    .append((String) entry.getValue()).toString());
            builder.append(System.getProperty("line.separator"));
        }
        builder.append("type").append(":").append(this.type).append(System.getProperty("line.separator"));
        builder.append(System.getProperty("line.separator"));
        for (Field field : this.fields) {
            builder.append(field.toString());
        }
        builder.append(System.getProperty("line.separator"));
        return builder.toString();
    }

    public static enum Type {
        INSERT(0), DELETE(1), UPDATE(2), BEGIN(3), COMMIT(4), DDL(5), REPLACE(6), HEARTBEAT(7), CONSISTENCY_TEST(8),
        ROLLBACK(9), DML(10), UNKNOWN(11);

        final int intValue;

        private Type(int value) {
            this.intValue = value;
        }

        public int value() {
            return this.intValue;
        }

        public static Type valueOf(int value) {
            for (Type type : values()) {
                if (type.value() == value) {
                    return type;
                }
            }
            return UNKNOWN;
        }
    }

    public void setFieldList(List<com.bce.dts.protobuf.FieldOuterClass.Field> list) {
        // TODO Auto-generated method stub
        for (com.bce.dts.protobuf.FieldOuterClass.Field field : list) {
            Field newField = new Field();
            newField.name = field.getFieldName();
            newField.strType = field.getFieldType();
            newField.encoding = field.getFieldCharset();
            newField.valueBefore = field.getValueBefore();
            newField.valueAfter = field.getValueAfter();
            
            this.fields.add(newField);
        }
    }
}
