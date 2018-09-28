package com.bce.dts.http;

import com.alibaba.fastjson.JSONObject;
import com.bce.dts.consumer.RegionContext;
import com.bce.dts.http.HttpClient;
import com.bce.dts.http.HttpResponse;

import junit.framework.TestCase;

public class HttpTest extends TestCase {
    public void testHttpClient() {
        RegionContext regionContext = new RegionContext();
        regionContext.setAccessKey("4d5743bfdaa84c19bc3f5ae352417311");
        regionContext.setSecretKey("ad36a5bb35a346e4959e253c6719d5fd");;
        String strUrl = "/json-api/v1/dts/dtsmly45zaomlu1dj9kc";
        HttpClient client = new HttpClient(regionContext);
        
        try {
            HttpResponse response = client.get(strUrl);
            JSONObject jsStr = JSONObject.parseObject(response.getString());
            System.out.println(response.getString());
            System.out.println(jsStr.getJSONObject("dtsTask").getString("taskName"));
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
        
    }
}
