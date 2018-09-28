package com.bce.dts.auth;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.bce.dts.auth.IAMService;
import com.bce.dts.http.MethodType;

import junit.framework.TestCase;

public class IAMServiceTest extends TestCase {
    
    /**
     * test function hashHmac
     */
    public void testHashHmac() {
        String strKey = "hello";
        String strData = "world";
        String strSign = IAMService.hashHmac(strData, strKey);
        assertEquals("f1ac9702eb5faf23ca291a4dc46deddeee2a78ccdaf0a412bed7714cfffb1cc4", strSign);
    }
    /**
     * test function normalizeString
     */
    public void testNormalizeString() {
        assertEquals("hello+world", IAMService.normalizeString("hello world"));
        assertEquals("%E4%B8%AD%E5%9B%BD%E4%BA%BA", IAMService.normalizeString("中国人"));
    }
    /**
     * test function getFormattedHeader
     */
    public void testGetFormattedHeader() {
        Map<String, String> mapHeader = new Hashtable<String, String>();
        mapHeader.put("x-bce-data", "2017-08-08T08:08:08Z");
        mapHeader.put("x-bce-authorazation", "helloworld");
        mapHeader.put("errHeader", "errHeader");
        mapHeader.put("content-type", "utf8");
        String strExpect = "content-type:utf8\nx-bce-authorazation:helloworld\nx-bce-data:2017-08-08T08%3A08%3A08Z";
        List<String> listSignedHeader = new ArrayList<String>();
        List<String> listSignedHeaderExpect = new ArrayList<String>();
        listSignedHeaderExpect.add("content-type");
        listSignedHeaderExpect.add("x-bce-authorazation");
        listSignedHeaderExpect.add("x-bce-data");
        assertEquals(strExpect, IAMService.getFormattedHeader(mapHeader, listSignedHeader));
        assertEquals(listSignedHeaderExpect, listSignedHeader);
        assertEquals("", IAMService.getFormattedHeader(null, listSignedHeader));
    }
    /**
     * test function getFormattedQueryString
     */
    public void testGetFormattedQueryString() {
        Map<String, String> mapParams = new Hashtable<String, String>();
        mapParams.put("k1", "v1");
        mapParams.put("k2", "v2");
        mapParams.put("authorization", "v3");
        String strExcept = "k1=v1&k2=v2";
        assertEquals(strExcept, IAMService.getFormattedQueryString(mapParams));
        assertEquals("", IAMService.getFormattedQueryString(null));
    }
    /**
     * test function getUTCTimeStr
     */
    public void testGetUTCTimeStr() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = formatter.format(new Date()) + "T";
        assertEquals(dateString, IAMService.getUTCTimeStr().substring(0, 11));
    }
    /**
     * test function getAuthorization
     */
    public void testgetAuthorization() {
        String strAk = "4d5743bfdaa84c19bc3f5ae352417311";
        String strSk = "ad36a5bb35a346e4959e253c6719d5fd";
        String strUrl = "/json-api/v1/dts/dtsmax4t50qpgx06jde5";
        Map<String, String> mapHeader = new Hashtable<String, String>();
        mapHeader.put("host", "cp01-dba-space-ssdtst-500.cp01.baidu.com:8080");
        mapHeader.put("x-bce-date", "2017-08-08T08:08:08Z");
        String strAuthorization = IAMService.createAuthorization(strAk, strSk, MethodType.GET, strUrl, null, mapHeader);
        System.out.println(strAuthorization);
    }
}