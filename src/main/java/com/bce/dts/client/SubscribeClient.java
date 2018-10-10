package com.bce.dts.client;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.bce.dts.consumer.ConsumerListener;
import com.bce.dts.endpoints.PubserverEndpoint;
import com.bce.dts.http.HttpClient;
import com.bce.dts.http.HttpResponse;
import com.bce.dts.http.MethodType;
import com.bce.dts.protobuf.AckOuterClass.Ack;
import com.bce.dts.protobuf.AckOuterClass.Ack.AckType;
import com.bce.dts.protobuf.AuthOuterClass.Auth;
import com.bce.dts.protobuf.Common.MsgType;
import com.bce.dts.util.NetIoHelper;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

/**
 * publish client, connect to pub server
 * @author yushaozai@baidu.com
 * @data 2017-08-10
 * @version 1.0
 */
public class SubscribeClient extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(SubscribeClient.class);

    /**
     * max times of reconnecting to pubserver
     */
    private static final int MAX_TIMES_RECONNECT = 3;
    /**
     * max times of retrying to get meta from DTS controller
     */
    private static final int MAX_TIMES_GET_META = 3;
    
    public enum ClientState {
        UNCONNECTD,
        CONNECTED,
        AUTH_OK,
        AUTH_FAIL,
        SUBSCRIBE
    }
    
    /**
     * subscribe client state
     */
    private ClientState clientState;
    /**
     * socket with connection to pubServer
     */
    private Socket socket;
    /**
     * socket output stream
     */
    private CodedOutputStream output;
    /**
     * socket input stream
     */
    private CodedInputStream input;
    /**
     * control subscribe client thread to exit
     */
    private boolean isExit;
    /**
     * consumer listeners
     */
    private List<ConsumerListener> listeners;
    /**
     * httpClient
     */
    private HttpClient httpClient;
    /**
     * subscribe dts task id
     */
    private String dtsId;
    /**
     * task admin user id
     */
    private String userId;
    /**
     * pubServer endpoint
     */
    private PubserverEndpoint pubServerEndpoint;
    /**
     * reciever client
     */
    private RecieverClient recieverClient;

    private Proxy proxy;

    /**
     * constructor
     * 
     * @param pubEndpoint      pubserver endpoint
     * @throws Exception
     */
    public SubscribeClient(HttpClient httpClient,
                           List<ConsumerListener> listeners,
                           String dtsId,
                           String userId,
                           Proxy proxy) {
        this.isExit = false;
        this.dtsId = dtsId;
        this.userId = userId;
        this.listeners = listeners;
        this.httpClient = httpClient;
        this.pubServerEndpoint = new PubserverEndpoint();
        this.clientState = ClientState.UNCONNECTD;
        this.proxy = proxy;
    }

    public SubscribeClient(HttpClient httpClient, List<ConsumerListener> listeners, String dtsId, String userId) {
        this(httpClient, listeners, dtsId, userId, null);
    }
    
    public SubscribeClient(HttpClient httpClient, List<ConsumerListener> listeners, String dtsId) {
        this(httpClient, listeners, dtsId, "");
    }
    
    /**
     * get codedInputStream
     * 
     * @return
     */
    public CodedInputStream getCodedInputStream() {
        return this.input;
    }
    
    /**
     * get codedOutputStream
     * 
     * @return
     */
    public CodedOutputStream getCodedOutputStream() {
        return this.output;
    }
    
    /**
     * connect to pubserver
     * 
     * @return true if connect successfully, or false
     * @throws Exception
     */
    private boolean connectPubServer() throws Exception {
        int retry = 0;
        String errmsg = null;
        String host = this.pubServerEndpoint.getStrIp();
        int port = this.pubServerEndpoint.getIntPort();
        
        while (retry++ < MAX_TIMES_RECONNECT) { 
            try {
                if ( null != proxy ) {
                    logger.debug("Use proxy: " + proxy.toString());
                    socket = new Socket(proxy);
                    socket.connect(new InetSocketAddress(host, port));
                } else {
                    socket = new Socket(host, port);
                }
                output = CodedOutputStream.newInstance(socket.getOutputStream());
                input = CodedInputStream.newInstance(socket.getInputStream());
                logger.debug("SubscribeClient: connect to pubServer successfully");
                this.clientState = ClientState.CONNECTED;
                break;
            }
            catch (Exception e) {
                logger.warn("SubscribeClient: connect to PubServer fail, errmsg:{}, retry:{}", e.getMessage(), retry);
                errmsg = e.getMessage();
            }
        }
        if (errmsg != null) {
            logger.error(errmsg);
            return false;
        }
        return true;
    }

    /**
     * client thread start to run
     */
    public void run() {
        logger.debug("start to run subscribe client");
        
        while (true) {
            try {
                int retry = 0;
                
                // connect to pubserver
                while (retry++ < MAX_TIMES_GET_META) {
                    logger.debug("SubscribeClient: getPubserverEndpoint, retry:{}", retry);
                    // get PubServer endpoint
                    this.getPubserverEndpoint();
                    
                    if (this.connectPubServer()) {
                        break;
                    }
                }
                if (this.clientState == ClientState.UNCONNECTD) {
                    logger.error("SubscribeClient: can't connect to available pubserver," 
                        + "please contact customer service");
                    return;
                }
                // send auth to pubserver
                logger.debug("send auth to pubserver");
                if (!this.sendAuth()) {
                    logger.error("SubscribeClient: send auth to pubserver failed");
                    return;
                }
                logger.debug("SubscribeClient: sdk auth pass");
                if (this.clientState != ClientState.AUTH_OK) {
                    logger.error("SubscribeClient: authorization failure from pubserver, please check ak/sk");
                    return;
                }
                
                this.recieverClient = new RecieverClient(this.pubServerEndpoint, this.listeners, this.dtsId, this.proxy);
                this.recieverClient.start();

                while (!isExit) {
                    if (!this.sendRealTimeProgress()) {
                        logger.error("SubscribeClient: send progress ack to pubserver failed");
                        break;
                    }
                    sleep(10000);
                }
                
            } catch (Exception e) {
                logger.error(e.getMessage());
                // if exception when communication with pubserver, retry to connect
                if (!e.getMessage().equals("connect to pubserver fail")) {
                    return;
                }
            }
        }
    }
    
    /**
     * exit subscibe client thread
     * @throws Exception 
     */
    public void clientExit() throws Exception {
        if (!this.sendConnClose()) {
            logger.error("SubscribeClient: send connection close message to pubserver failded");
            return;
        }
        logger.debug("SubscribeClient: notify subscibe client thread to exit");
        this.recieverClient.clientExit();
        this.recieverClient.join();
        this.isExit = true;
        this.join();
        logger.debug("SubscribeClient: subscibe client thread exits");
    }

    /**
     * get transfer endpoint
     * 
     * @throws Exception 
     */
    private void getPubserverEndpoint() throws Exception {
        String strUrl = "/json-api/v1/dts/" + this.dtsId + "?subMeta";
        logger.debug("SubscribeClient: getPubserverEndpoint, url:{}", strUrl);
        
        HttpResponse response = this.httpClient.get(strUrl);
        JSONObject jsStr = JSONObject.parseObject(response.getString());
        logger.debug("SubscribeClient: getPubserverEndpoint, response:{}", response.getString());
        
        if (jsStr.getJSONObject("pubServer") == null) {
            logger.error("SubscribeClient: get pubserver metadata failed, url:{}", strUrl);
            throw new Exception("get pubserver metadata failed");
        }
        if (jsStr.getJSONObject("pubServer").getString("host").equals("")) {
            String errmsg = "pubserver host is empty, please check dts task from console";
            logger.error(errmsg);
            throw new Exception(errmsg);
        }
        if (jsStr.getJSONObject("pubServer").getInteger("port") == 0) {
            String errmsg = "pubserver port is 0, please check dts task from console";
            logger.error(errmsg);
            throw new Exception(errmsg);
        }
        this.pubServerEndpoint.setStrIp(jsStr.getJSONObject("pubServer").getString("host"));
        this.pubServerEndpoint.setIntPort(jsStr.getJSONObject("pubServer").getInteger("port"));
        logger.debug("getPubserverEndpoint, host:{}", this.pubServerEndpoint.toString());
    }
    /**
     * send auth message to pubserver to check
     * 
     * @return        true if send successfully, or false
     */
    private boolean sendAuth() {
        try {
            String url = "/pri-api/v1/dts/iamAuth";
            logger.debug("create header");
            Map<String, String> httpHeader = this.httpClient.createHeader(url, MethodType.POST);
            logger.debug("new builder");
            Auth auth = Auth.newBuilder()
                    .setHost(httpHeader.get("host"))
                    .setBceDate(httpHeader.get("x-bce-date"))
                    .setAuthorization(httpHeader.get("authorization"))
                    .setPath(httpHeader.get("path"))
                    .setDtsId(this.dtsId)
                    .setType(MsgType.AUTH)
                    .build();

            // send auth message to pubserver
            logger.debug("send auth message to pubserver " + auth.toString());
            NetIoHelper.sendMessage(auth, this.output);

            // recv ack message from pubserver
            logger.debug("recv ack message from pubserver");
            Ack ack = (Ack) NetIoHelper.recieveMessage(MsgType.ACK_VALUE, this.input);
            
            // set client state
            logger.debug("set client state");
            if (ack.getAckType() == AckType.AUTH_OK) {
                this.clientState = ClientState.AUTH_OK;
            }
            else if (ack.getAckType() == AckType.AUTH_FAIL) {
                this.clientState = ClientState.AUTH_FAIL;
            }
            else {
                throw new Exception("ack type is invalid");
            }
            return true;
        }
        catch (Exception e) {
            logger.error("SubscribeClient: send auth to pubserver exception: {}", e.getMessage());
        }
        return false;
    }

    /**
     * Send real time progress to pubserver
     * @return true if send successfully, or false
     */
    private boolean sendRealTimeProgress() {
        try {
            Ack ack = Ack.newBuilder()
                    .setType(MsgType.ACK)
                    .setAckType(AckType.COMMIT_OFFSET)
                    .setDtsId(this.dtsId)
                    .setOffset(this.recieverClient.getOffset())
                    .setPosition(this.recieverClient.getPosition())
                    .setTimestamp(this.recieverClient.getTimestamp())
                    .build();
            // send ack message to pubserver
            NetIoHelper.sendMessage(ack, this.output);
            return true;
        }
        catch (Exception e) {
            logger.error("SubscribeClient: send real time progress to pubserver exception: {}", e.getMessage());
        }
        return false;        
    }
    

    private boolean sendConnClose() {
        // TODO Auto-generated method stub
        try {
            Ack ack = Ack.newBuilder()
                    .setType(MsgType.ACK)
                    .setAckType(AckType.CONN_CLOSE)
                    .setDtsId(this.dtsId)
                    .setOffset("")
                    .setPosition("")
                    .build();
            // send ack message to pubserver
            NetIoHelper.sendMessage(ack, this.output);
            return true;
        }
        catch (Exception e) {
            logger.error("SubscribeClient: send connection close ack to pubserver exception: {}", e.getMessage());
        }
        return false;
    }
}
