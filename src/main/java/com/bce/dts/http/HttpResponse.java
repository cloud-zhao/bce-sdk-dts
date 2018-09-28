package com.bce.dts.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;

import org.apache.http.util.EntityUtils;

/**
 * http response
 * 
 * @author yushaozai@baidu.com
 * @data 2017-01-05
 * @version 1.0
 */
public class HttpResponse extends HttpRequest {
    
    private int status;

    /**
     * constructor
     * 
     * @param strUrl
     */
    public HttpResponse(String strUrl) {
        super(strUrl);
    }
    /**
     * constructor
     */
    public HttpResponse() {
    }
    /**
     * constructor
     *   
     * @param response
     * @throws IllegalStateException
     * @throws IOException
     */
    public HttpResponse(org.apache.http.HttpResponse response) throws IllegalStateException, IOException {
        this.status = response.getStatusLine().getStatusCode();
      
        try {
            // this.content = new BasicResponseHandler().handleResponse(response).getBytes();
            this.content = EntityUtils.toByteArray(response.getEntity());
        } catch (Exception e) {
            this.content = e.getMessage().getBytes();
        }
    }
    
    /**
     * set content
     * 
     * @param content
     * @param encoding
     * @param format
     */
    public void setContent(byte[] content, String encoding, FormatType format) {
        this.content = content;
        this.encoding = encoding;
        this.contentType = format;
    }
    /**
     * get header value
     * 
     * @param name
     */
    public String getHeaderValue(String name) {
        String value = this.headers.get(name);
        if (null == value) {
            value = this.headers.get(name.toLowerCase());
        }
        return value;
    }
    /**
     * read http content
     * 
     * @param content
     * @return     http content of byte array format
     * @throws IOException
     */
    private static byte[] readContent(InputStream content) throws IOException {
        if (content == null) {
            return null;
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buff = new byte[1024];
    
        while (true) {
            int read = content.read(buff);
            if (read == -1) {
                break;
            }
            outputStream.write(buff, 0, read);
        }

        return outputStream.toByteArray();
    }
    /**
     * parser http connection
     * 
     * @param response
     * @param httpConn
     * @param content
     * @throws IOException
     */
    private static void pasrseHttpConn(HttpResponse response, HttpURLConnection httpConn, InputStream content) 
            throws IOException {
        byte[] buff = readContent(content);
        response.setStatus(httpConn.getResponseCode());
        Map<String, List<String>> headers = httpConn.getHeaderFields();
    
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            String key = entry.getKey();
      
            if (null != key) {
                List<String> values = entry.getValue();
                StringBuilder builder = new StringBuilder(values.get(0));
        
                for (int i = 1; i < values.size(); i++) {
                    builder.append(",");
                    builder.append(values.get(i));
                }
                response.putHeaderParameter(key, builder.toString());
            }
        }
        String type = response.getHeaderValue("Content-Type");
    
        if ((null != buff) && (null != type)) {
            response.setEncoding("UTF-8");
            String[] split = type.split(";");
            response.setContentType(FormatType.mapAcceptToFormat(split[0].trim()));
      
            if ((split.length > 1) && (split[1].contains("="))) {
                String[] codings = split[1].split("=");
                response.setEncoding(codings[1].trim().toUpperCase());
            }
        }
        response.setStatus(httpConn.getResponseCode());
        response.setContent(buff, response.getEncoding(), response.getContentType());
    }
    /**
     * get http response
     * 
     * @param request
     * @return http response
     * @throws IOException
     */
    public static HttpResponse getResponse(HttpRequest request) throws IOException {
        OutputStream out = null;
        InputStream content = null;
        HttpResponse response = null;
        HttpURLConnection httpConn = request.getHttpConnection();
    
        try {
            httpConn.connect();
            if (null != request.getContent()) {
                out = httpConn.getOutputStream();
                out.write(request.getContent());
            }
            content = httpConn.getInputStream();
            response = new HttpResponse(httpConn.getURL().toString());
            pasrseHttpConn(response, httpConn, content);
            return response;
        } catch (IOException e) {
            content = httpConn.getErrorStream();
            response = new HttpResponse(httpConn.getURL().toString());
            pasrseHttpConn(response, httpConn, content);
            return response;
        } finally {
            if (content != null) {
                content.close();
            }
            httpConn.disconnect();
        }
    }
    /**
     * get http status code
     * 
     * @return status code
     */
    public int getStatus() {
        return this.status;
    }
    /**
     * set http status code
     * 
     * @param status
     */
    public void setStatus(int status) {
        this.status = status;
    }
    /**
     * check if http request is successful
     * 
     * @return    true if successful, or false
     */
    public boolean isSuccess() {
        if ((200 <= this.status) && (300 > this.status)) {
            return true;
        }
        return false;
    }
    /**
     * get http content of string format
     * 
     * @return    http content of string format
     */
    public String getString() {
        return new String(this.content);
    }
}

