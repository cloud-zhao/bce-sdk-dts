package com.bce.dts.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.UUID;

/**
 * parameter helper
 * 
 * @author yushaozai@baidu.com
 * @data 2017-01-05
 * @version 1.0
 */
public class ParameterHelper {

    private static final String TIME_ZONE = "GMT";
    private static final String FORMAT_ISO8601 = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final String FORMAT_RFC2616 = "EEE, dd MMM yyyy HH:mm:ss zzz";

    /**
     * get uuid
     * 
     * @return uuid
     */
    public static String getUniqueNonce() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }

    /**
     * get iso8601 time
     * 
     * @param date
     * @return iso8601 time
     */
    public static String getISO8601Time(Date date) {
        Date nowDate = date;
        if (null == date) {
            nowDate = new Date();
        }
        SimpleDateFormat df = new SimpleDateFormat(FORMAT_ISO8601);
        df.setTimeZone(new SimpleTimeZone(0, TIME_ZONE));

        return df.format(nowDate);
    }

    /**
     * get rfc2616 date
     * 
     * @param date
     * @return rfc2616 date
     */
    public static String getRFC2616Date(Date date) {
        Date nowDate = date;
        if (null == date) {
            nowDate = new Date();
        }
        SimpleDateFormat df = new SimpleDateFormat(FORMAT_RFC2616,
                Locale.ENGLISH);
        df.setTimeZone(new SimpleTimeZone(0, TIME_ZONE));
        return df.format(nowDate);
    }

    /**
     * parser date
     * 
     * @param strDate
     * @return parsered date
     * @throws ParseException
     */
    public static Date parse(String strDate) throws ParseException {
        if ((null == strDate) || ("".equals(strDate))) {
            return null;
        }
        try {
            return parseISO8601(strDate);
        } catch (ParseException exp) {
            // just continue
        }
        return parseRFC2616(strDate);
    }

    /**
     * parser iso8601 date
     * 
     * @param strDate
     * @return iso8601 date
     * @throws ParseException
     */
    public static Date parseISO8601(String strDate) throws ParseException {
        if ((null == strDate) || ("".equals(strDate))) {
            return null;
        }
        SimpleDateFormat df = new SimpleDateFormat(FORMAT_ISO8601);
        df.setTimeZone(new SimpleTimeZone(0, TIME_ZONE));
        return df.parse(strDate);
    }

    /**
     * parser rfc2616 date
     * 
     * @param strDate
     * @return rfc2616 date
     * @throws ParseException
     */
    public static Date parseRFC2616(String strDate) throws ParseException {
        if ((null == strDate) || ("".equals(strDate))
                || (strDate.length() != FORMAT_RFC2616.length())) {
            return null;
        }
        SimpleDateFormat df = new SimpleDateFormat(FORMAT_RFC2616,
                Locale.ENGLISH);
        df.setTimeZone(new SimpleTimeZone(0, TIME_ZONE));
        return df.parse(strDate);
    }

    /**
     * make md5 sum
     * 
     * @param buff
     * @return md5sum
     * @throws NoSuchAlgorithmException
     */
    public static String md5Sum(byte[] buff) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] messageDigest = md.digest(buff);
        return Base64Helper.encode(messageDigest);
    }
}
