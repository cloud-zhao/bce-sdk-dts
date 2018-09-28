package com.bce.dts.auth;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang3.StringUtils;

import com.bce.dts.http.MethodType;

public class IAMService {
    private static final int API_VERSION = 1;
    private static final String HASH_FUNC = "HmacSHA256";
    private static final String USER_DEFINE_HEADER_PREFIX = "x-bce-";
    private static final int AUTH_EXPIRATION_IN_SECONDS = 1800;
    
    /**
     * get authorization signature for send http request to DTS
     * @param strAk            bce user access key
     * @param strSk            bce user secret key
     * @param method        MethodType enum object
     * @param strUrl           http request url
     * @param mapParams        http request querystring params
     * @param mapHeader        http request header
     * @return              authorization signature
     */
    public static String createAuthorization(String strAk, String strSk, MethodType enumMethodType, String strUrl,
            Map<String, String> mapParams, Map<String, String> mapHeader) {
        String strAuthorization = null;
        StringBuffer strToSign = new StringBuffer();
        List<String> listSignedHeader = new ArrayList<String>();
        String strUtcTime = getUTCTimeStr();
        String strSignKeyInfo = String.format("bce-auth-v%d/%s/%s/%d", 
                API_VERSION, strAk, strUtcTime, AUTH_EXPIRATION_IN_SECONDS);
        String strFormattedQueryStr = getFormattedQueryString(mapParams);
        String strFormattedHeader = getFormattedHeader(mapHeader, listSignedHeader);
        strToSign.append(enumMethodType.toString()).append("\n").append(strUrl).append("\n");
        strToSign.append(strFormattedQueryStr).append("\n").append(strFormattedHeader);
        String strSignKey = hashHmac(strSignKeyInfo, strSk);
        String strSignResult = hashHmac(strToSign.toString(), strSignKey);
        strAuthorization = String.format("%s/%s/%s",
                strSignKeyInfo, StringUtils.join(listSignedHeader.toArray(), ";"), strSignResult);
        
        return strAuthorization;
        
    }
    
    /**
     * get current utc time, format is "yyyy-MM-ddTHH:mm:ssZ"
     * @return    utf time, or null if failed
     */
    public static String getUTCTimeStr() {
        Calendar cal = Calendar.getInstance() ;  
        int intZoneOffset = cal.get(Calendar.ZONE_OFFSET);  
        int intDstOffset = cal.get(Calendar.DST_OFFSET);  
        cal.add(Calendar.MILLISECOND, -(intZoneOffset + intDstOffset));
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(cal.getTime());
        
        return dateString.replaceAll(" ", "T") + "Z";
    }
    /**
     * get formatted query string, format is "k1=v1&k2=v2&..."
     * @param param     query string parameters
     * @return          formatted query string
     */
    public static String getFormattedQueryString(Map<String, String> mapParams) {
        List<String> listFormattedString = new ArrayList<String>();
        
        if (mapParams == null) {
            return "";
        }
        for (Map.Entry<String, String> entry : mapParams.entrySet()) {  
            if (entry.getKey().equals("authorization")) {
                continue;
            }
            StringBuffer sbString = new StringBuffer();
            sbString.append(normalizeString(entry.getKey())).append("=").append(normalizeString(entry.getValue()));
            listFormattedString.add(sbString.toString());
        }
        Collections.sort(listFormattedString);
        return StringUtils.join(listFormattedString, "&");
    }
    /**
     * get formatted header, format is "k1:v1\nk2:v2\n..."
     * @param mapHeader            http request header
     * @param listSignedHeader      signed header
     * @return                  formatted header
     */
    public static String getFormattedHeader(Map<String, String> mapHeader, List<String> listSignedHeader) {
        List<String> listFormattedHeader = new ArrayList<String>();
        List<String> listStandardHeader = new ArrayList<String>();
        listStandardHeader.add("host");
        listStandardHeader.add("content-md5");
        listStandardHeader.add("content-length");
        listStandardHeader.add("content-type");
        
        if (mapHeader == null) {
            return "";
        }
        for (Map.Entry<String, String> entry : mapHeader.entrySet()) {
            String strKey = entry.getKey().toLowerCase().replace("_", "-");
            
            if (strKey.indexOf(USER_DEFINE_HEADER_PREFIX) == 0 || listStandardHeader.contains(strKey)) {
                StringBuffer sbHeader = new StringBuffer();     
                sbHeader.append(strKey).append(":").append(normalizeString(entry.getValue()));
                listFormattedHeader.add(sbHeader.toString());
                listSignedHeader.add(strKey);
            }
        }
        Collections.sort(listFormattedHeader);
        Collections.sort(listSignedHeader);
        return StringUtils.join(listFormattedHeader.toArray(), "\n");
    }
    /**
     * normalize string, urlencode(utf8(str))
     * @param str       string to be normalized
     * @return          encoded string
     */
    public static String normalizeString(String str) {
        try {
            return URLEncoder.encode(str, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * generate sha256 hash value
     * @param strData
     * @param strKey
     * @return      hash value
     */
    public static String hashHmac(String strData, String strKey) {
        try {
            SecretKeySpec signingKey = new SecretKeySpec(strKey.getBytes(), HASH_FUNC);
            Mac mac = Mac.getInstance(HASH_FUNC);
            mac.init(signingKey);
            byte[] rawHmac = mac.doFinal(strData.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : rawHmac) {
                sb.append(byteToHexString(b));
            }
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    /**
     * convert byte to hex string
     * @param ib    byte data
     * @return      hex string
     */
    private static String byteToHexString(byte ib) {
        char[] digit = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        char[] ob = new char[2];
        ob[0] = digit[(ib >>> 4) & 0X0f];
        ob[1] = digit[ib & 0X0F];
        return new String(ob);
    }
}
