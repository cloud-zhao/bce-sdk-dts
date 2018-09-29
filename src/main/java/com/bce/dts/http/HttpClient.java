package com.bce.dts.http;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

import com.bce.dts.auth.IAMService;
import com.bce.dts.consumer.RegionContext;
import com.bce.dts.endpoints.ControllerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * http client handler for executing get/post request
 * 
 * @author yushaozai@baidu.com
 * @data 2017-08-05
 * @version 1.0
 */
@SuppressWarnings({ "deprecation" })
public class HttpClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

    private static final String DefaultEncoding = "UTF-8";
    private static final int DefaultConnectionTimeout = 30000;
    private static final int DefaultSocketTimeout = 30000;
    private org.apache.http.client.HttpClient client;

    private RegionContext regionContext;


    /**
     * constructor
     * 
     * @param  regionContext proxy
     */
    public HttpClient(RegionContext regionContext, Proxy proxy) {
        this.regionContext = regionContext;
        HttpClientBuilder builder;
        RequestConfig config = RequestConfig.custom()
                .setConnectionRequestTimeout(DefaultConnectionTimeout)
                .setSocketTimeout(DefaultSocketTimeout)
                .setConnectTimeout(DefaultConnectionTimeout).build();
        if (null != proxy) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) proxy.address();
            HttpHost host = new HttpHost(inetSocketAddress.getHostName(), inetSocketAddress.getPort());
            builder = HttpClientBuilder.create().setProxy(host);
        } else {
            builder = HttpClientBuilder.create();
        }

        this.client = builder.setDefaultRequestConfig(config).build();
        logger.debug("init: " + proxy);
    }

    public HttpClient(RegionContext regionContext) {
        this(regionContext, null);
    }

    /**
     * create http header
     * 
     * @param strUrl
     * @param methodType
     * @return http header
     */
    public Map<String, String> createHeader(String strUrl, MethodType methodType) {
        Map<String, String> mapHeader = new Hashtable<String, String>();
        try {
            String strApiHost = ControllerEndpoint.getDns();
            String strAk = this.regionContext.getAccessKey();
            String strSk = this.regionContext.getSecretKey();
            InetAddress addr = InetAddress.getLocalHost();
            String strHost = addr.getHostAddress();
            String strUtcTime = IAMService.getUTCTimeStr();
            StringBuffer sbUrl = new StringBuffer(strUrl);
            Map<String, String> mapParam = getQueryString(sbUrl);
            mapHeader.put("host", strApiHost);
            mapHeader.put("x-bce-date", strUtcTime);
            mapHeader.put("authorization",
                    IAMService.createAuthorization(
                            strAk, 
                            strSk, 
                            methodType, 
                            sbUrl.toString(), 
                            mapParam, 
                            mapHeader));
            mapHeader.put("path", "http://" + strApiHost + strUrl);
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return mapHeader;
    }

    /**
     * get query string
     * 
     * @param strUrl
     * @return
     */
    private Map<String, String> getQueryString(StringBuffer sbUrl) {
        if (sbUrl.indexOf("?") == -1) {
            return null;
        }
        Map<String, String> mapParam = new Hashtable<String, String>();
        String[] arrStrs = sbUrl.toString().split("\\?");
        String[] arrParams = arrStrs[1].split("&");
        sbUrl.delete(0, sbUrl.length()).append(arrStrs[0]);

        for (int i = 0; i < arrParams.length; i++) {
            String[] kv = arrParams[i].split("=");
            if (kv.length == 2) {
                mapParam.put(kv[0], kv[1]);
            } else {
                mapParam.put(kv[0], "");
            }
        }
        return mapParam;
    }

    /**
     * http request of get method
     * 
     * @param path
     * @return http response
     * @throws IOException
     */
    public HttpResponse get(String strUrl) throws IOException {

        Map<String, String> mapHeader = this.createHeader(strUrl, MethodType.GET);
        String path = mapHeader.get("path");
        mapHeader.remove("path");
        HttpGet method = new HttpGet(path);

        if (mapHeader != null) {
            for (Map.Entry<String, String> entry : mapHeader.entrySet()) {
                method.addHeader(entry.getKey(), entry.getValue());
            }
        }
        org.apache.http.HttpResponse response = client.execute(method);
        HttpResponse resp = new HttpResponse(response);
        method.releaseConnection();

        if (resp.getStatus() != 200) {
            throw new IllegalStateException(
                    "GET request to " + path + " failed: " + resp.getString() + ", code: " + resp.getStatus());
        }
        return resp;
    }

    /**
     * 
     * @param url
     * @param posts
     * @return
     * @throws IllegalStateException
     * @throws IOException
     */
    public HttpResponse post(String strUrl, Map<String, String> posts) throws IllegalStateException, IOException {
        List<BasicNameValuePair> parameters = new ArrayList<BasicNameValuePair>();
        for (Map.Entry<String, String> pair : posts.entrySet()) {
            parameters.add(new BasicNameValuePair(pair.getKey(), pair.getValue()));
        }
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(parameters, DefaultEncoding);
        Map<String, String> mapHeader = this.createHeader(strUrl, MethodType.GET);
        String path = mapHeader.get("path");
        mapHeader.remove("path");
        HttpPost method = new HttpPost(path);
        method.setEntity(entity);

        if (mapHeader != null) {
            for (Map.Entry<String, String> entry : mapHeader.entrySet()) {
                method.addHeader(entry.getKey(), entry.getValue());
            }
        }
        HttpResponse resp = new HttpResponse(client.execute(method));
        method.releaseConnection();

        if (resp.getStatus() != 200) {
            throw new IllegalStateException(
                    "POST Request to " + path + " failed: " + resp.getString() + ", code: " + resp.getStatus());
        }
        return resp;
    }
}
