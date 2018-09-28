package com.bce.dts.http;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.bce.dts.util.ParameterHelper;

/**
 * http request
 * 
 * @author yushaozai@baidu.com
 * @data 2017-01-05
 * @version 1.0
 */
public class HttpRequest {
    
    private String url = null;
    private MethodType method = null;
    protected FormatType contentType = null;
    protected byte[] content = null;
    protected String encoding = null;
    protected Map<String, String> headers = null;
    
    /**
     * constructor
     * 
     * @param strUrl
     */
    public HttpRequest(String strUrl) {
        this.url = strUrl;
        this.headers = new HashMap<String, String>();
    }
    /**
     * constructor
     * 
     * @param strUrl
     * @param tmpHeaders
     */
    public HttpRequest(String strUrl, Map<String, String> tmpHeaders) {
        this.url = strUrl;
        if (null != tmpHeaders) {
            this.headers = tmpHeaders;
        }
    }
    /**
     * default constructor
     */
    public HttpRequest() {
    }

    /**
     * getter
     * @return http url
     */
    public String getUrl() {
        return this.url;
    }
    /**
     * setter
     * 
     * @param url
     */
    protected void setUrl(String url) {
        this.url = url;
    }
    /**
     * getter
     * 
     * @return encoding
     */
    public String getEncoding() {
        return this.encoding;
    }
    /**
     * setter
     * 
     * @param encoding
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
    /**
     * getter
     * 
     * @return content type
     */
    public FormatType getContentType() {
        return this.contentType;
    }
    /**
     * setter
     * 
     * @param contentType
     */
    public void setContentType(FormatType contentType) {
        this.contentType = contentType;
    
        if (null != contentType) {
            this.headers.put("Content-Type", getContentTypeValue(this.contentType, this.encoding));
        }
        else {
            this.headers.remove("Content-Type");
        }
    }
    /**
     * getter
     * 
     * @return method
     */
    public MethodType getMethod() {
        return this.method;
    }
    /**
     * setter
     * 
     * @param method
     */
    public void setMethod(MethodType method) {
        this.method = method;
    }
    /**
     * get content
     * 
     * @return content
     */
    public byte[] getContent() {
        return this.content;
    }
    /**
     * get header value
     * 
     * @param name  header key name
     * @return      header name
     */
    public String getHeaderValue(String name) {
        return this.headers.get(name);
    }
    /**
     * set header 
     * 
     * @param name    header key name
     * @param value   header value
     */
    public void putHeaderParameter(String name, String value) {
        if ((null != name) && (null != value)) {
            this.headers.put(name, value);
        }
    }
    /**
     * set content
     * 
     * @param content
     * @param encoding
     * @param format
     * @throws NoSuchAlgorithmException
     */
    public void setContent(byte[] content, String encoding, FormatType format) throws NoSuchAlgorithmException {
        if (null == content) {
            this.headers.remove("Content-MD5");
            this.headers.remove("Content-Length");
            this.headers.remove("Content-Type");
            this.contentType = null;
            this.content = null;
            this.encoding = null;
            return;
        }
        this.content = content;
        this.encoding = encoding;
        String contentLen = String.valueOf(content.length);
        String strMd5 = ParameterHelper.md5Sum(content);
        if (null != format) {
            this.contentType = format;
        }
        else {
            this.contentType = FormatType.RAW;
        }
        this.headers.put("Content-MD5", strMd5);
        this.headers.put("Content-Length", contentLen);
        this.headers.put("Content-Type", getContentTypeValue(this.contentType, encoding));
    }
    /**
     * get headers
     * @return    http headers
     */
    public Map<String, String> getHeaders() {
        return Collections.unmodifiableMap(this.headers);
    }
    /**
     * get http content
     * @return    http content
     * @throws IOException
     */
    public HttpURLConnection getHttpConnection() throws IOException {
        Map<String, String> mappedHeaders = this.headers;
        String strUrl = this.url;
    
        if ((null == strUrl) || (null == this.method)) {
            return null;
        }
        URL url = null;
        String[] urlArray = null;
    
        if (MethodType.POST.equals(this.method)) {
            urlArray = strUrl.split("\\?");
            url = new URL(urlArray[0]);
        }
        else {
            url = new URL(strUrl);
        }
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        httpConn.setRequestMethod(this.method.toString());
        httpConn.setDoOutput(true);
        httpConn.setDoInput(true);
        httpConn.setUseCaches(false);

        for (Map.Entry<String, String> entry : mappedHeaders.entrySet()) {
            httpConn.setRequestProperty(entry.getKey(), entry.getValue());
        }

        if (null != getHeaderValue("Content-Type")) {
            httpConn.setRequestProperty("Content-Type", getHeaderValue("Content-Type"));
        }
        else {
            String contentTypeValue = getContentTypeValue(this.contentType, this.encoding);
            if (null != contentTypeValue) {
                httpConn.setRequestProperty("Content-Type", contentTypeValue);
            }
        }

        if ((MethodType.POST.equals(this.method)) && (urlArray.length == 2)) {
            httpConn.getOutputStream().write(urlArray[1].getBytes());
        }
        return httpConn;
    }
    /**
     * get content type value
     * 
     * @param contentType
     * @param encoding
     * @return
     */
    private String getContentTypeValue(FormatType contentType, String encoding) {
        if ((null != contentType) && (null != encoding)) {
            return FormatType.mapFormatToAccept(contentType) + ";charset=" + encoding.toLowerCase();
        }

        if (null != contentType) {
            return FormatType.mapFormatToAccept(contentType);
        }
        return null;
    }
}