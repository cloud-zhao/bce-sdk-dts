package com.bce.dts.util;

import java.io.UnsupportedEncodingException;

/**
 * base64 helper
 * 
 * @author yushaozai@baidu.com
 * @data 2017-01-06
 * @version 1.0
 */
public class Base64Helper {

    private static final String BASE64_CODE = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    private static final int[] BASE64_DECODE = { -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, 62, -1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1,
            -1, -1, -2, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
            13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1,
            -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
            41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1 };

    /**
     * zero padding
     * 
     * @param length
     * @param bytes
     * @return padded byte string
     */
    private static byte[] zeroPad(int length, byte[] bytes) {
        byte[] padded = new byte[length];
        System.arraycopy(bytes, 0, padded, 0, bytes.length);
        return padded;
    }

    /**
     * base64 encode
     * 
     * @param buff
     * @return encoded string
     */
    public static synchronized String encode(byte[] buff) {
        if (null == buff) {
            return null;
        }
        StringBuilder strBuilder = new StringBuilder("");
        int paddingCount = (3 - buff.length % 3) % 3;
        byte[] stringArray = zeroPad(buff.length + paddingCount, buff);
        for (int i = 0; i < stringArray.length; i += 3) {
            int j = ((stringArray[i] & 0xFF) << 16)
                    + ((stringArray[(i + 1)] & 0xFF) << 8)
                    + (stringArray[(i + 2)] & 0xFF);
            strBuilder.append(BASE64_CODE.charAt(j >> 18 & 0x3F));
            strBuilder.append(BASE64_CODE.charAt(j >> 12 & 0x3F));
            strBuilder.append(BASE64_CODE.charAt(j >> 6 & 0x3F));
            strBuilder.append(BASE64_CODE.charAt(j & 0x3F));
        }
        int intPos = strBuilder.length();
        for (int i = paddingCount; i > 0; i--) {
            strBuilder.setCharAt(intPos - i, '=');
        }

        return strBuilder.toString();
    }

    /**
     * encode string
     * 
     * @param string
     * @param encoding
     * @return encoded string
     * @throws UnsupportedEncodingException
     */
    public static synchronized String encode(String string, String encoding)
            throws UnsupportedEncodingException {
        if ((null == string) || (null == encoding)) {
            return null;
        }
        byte[] stringArray = string.getBytes(encoding);
        return encode(stringArray);
    }

    /**
     * decode string
     * 
     * @param string
     * @param encoding
     * @return decoded string
     * @throws UnsupportedEncodingException
     */
    public static synchronized String decode(String string, String encoding)
            throws UnsupportedEncodingException {
        if ((null == string) || (null == encoding)) {
            return null;
        }
        int posIndex = 0;
        int decodeLen = string.endsWith("=") ? string.length() - 1 :
            string.endsWith("==") ? string.length() - 2 : string.length();
        byte[] buff = new byte[decodeLen * 3 / 4];
        int count4 = decodeLen - decodeLen % 4;

        for (int i = 0; i < count4; i += 4) {
            int c0 = BASE64_DECODE[string.charAt(i)];
            int c1 = BASE64_DECODE[string.charAt(i + 1)];
            int c2 = BASE64_DECODE[string.charAt(i + 2)];
            int c3 = BASE64_DECODE[string.charAt(i + 3)];
            buff[(posIndex++)] = ((byte) ((c0 << 2 | c1 >> 4) & 0xFF));
            buff[(posIndex++)] = ((byte) (((c1 & 0xF) << 4 | c2 >> 2) & 0xFF));
            buff[(posIndex++)] = ((byte) (((c2 & 0x3) << 6 | c3) & 0xFF));
        }
        if (2 <= decodeLen % 4) {
            int c0 = BASE64_DECODE[string.charAt(count4)];
            int c1 = BASE64_DECODE[string.charAt(count4 + 1)];
            buff[(posIndex++)] = ((byte) ((c0 << 2 | c1 >> 4) & 0xFF));
            if (3 == decodeLen % 4) {
                int c2 = BASE64_DECODE[string.charAt(count4 + 2)];
                buff[(posIndex++)] = ((byte) (((c1 & 0xF) << 4 | c2 >> 2)
                        & 0xFF));
            }
        }
        return new String(buff, encoding);
    }
}