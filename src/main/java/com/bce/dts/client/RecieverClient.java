package com.bce.dts.client;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bce.dts.client.SubscribeClient.ClientState;
import com.bce.dts.consumer.ConsumerListener;
import com.bce.dts.endpoints.PubserverEndpoint;
import com.bce.dts.model.DataMessage;
import com.bce.dts.model.Record;
import com.bce.dts.protobuf.Common.MsgType;
import com.bce.dts.protobuf.ConnectOuterClass.Connect;
import com.bce.dts.protobuf.EventOuterClass.Event;
import com.bce.dts.util.NetIoHelper;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * reciever client, recieve events from pub server
 * @author yuxinwei01@baidu.com
 * @data 2018-03-19
 * @version 1.0
 */
public class RecieverClient extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(RecieverClient.class);
    
    /**
     * max times of reconnecting to pubserver transmission thread
     */
    private static final int MAX_TIMES_RECONNECT = 3;
    /**
     * max times of retrying to get meta from DTS controller
     */
    private static final int MAX_TIMES_GET_META = 3;
    /**
     * max times of retry when recieve message too large exception
     */
    private static final int MAX_TIMES_RETEY = 3;
    /**
     * max number of events in one TrxMessage
     */
    private static final int MAX_EVENT_COUNTER = 100;
    /**
     * timeout of waiting for get max number of event, unit is millisecond
     */
    private static final long MAX_EVENT_TIMEOUT_MS = 1000;
    
    /**
     * reciever client state
     */
    private ClientState clientState;
    /**
     * socket output stream
     */
    private CodedOutputStream output;
    /**
     * socket input stream
     */
    private CodedInputStream input;
    /**
     * socket with connection to pubServer
     */
    private Socket socket;
    /**
     * control subscribe client thread to exit
     */
    private boolean isExit;
    /**
     * consumer listeners
     */
    private List<ConsumerListener> listeners;
    /**
     * pubServer endpoint
     */
    private PubserverEndpoint pubServerEndpoint;
    /**
     * current recieved offset
     */
    private String offset;
    /**
     * current recieved binlog position
     */
    private String position;
    /**
     * current recieved timestamp
     */
    private String timestamp;
    /**
     * latest consumed offset
     */
    private String lastOffset;
    /**
     * latest consumed binlog position
     */
    private String lastPosition;
    /**
     * latest consumed event timestamp
     */
    private String lastTimestamp;
    /**
     * subscribe dts task id
     */
    private String dtsId;
    /**
     * retry times for InvalidProtocolBufferException
     */
    private int invalidProtocolRetry;

    private Proxy proxy;

    RecieverClient(PubserverEndpoint pEndpoint, List<ConsumerListener> listeners, String dtsId) {
        this(pEndpoint, listeners, dtsId, null);
    }
    
    RecieverClient(PubserverEndpoint pEndpoint, List<ConsumerListener> listeners, String dtsId, Proxy proxy) {
        this.pubServerEndpoint = pEndpoint;
        this.listeners = listeners;
        this.dtsId = dtsId;
        this.isExit = false;
        this.clientState = ClientState.UNCONNECTD;
        this.offset = "";
        this.position = "";
        this.timestamp = "";
        this.lastOffset = "";
        this.lastPosition = "";
        this.lastTimestamp = "";
        this.invalidProtocolRetry = 0;
        this.proxy = proxy;
    }

    public void run() {
        logger.debug("start to run reciever client");
        
        while (true) {
            try {
                int retry = 0;
                
                // connect to pubserver transmission thread
                while (retry++ < MAX_TIMES_GET_META) {
                    if (this.connectPubServer()) {
                        break;
                    }
                }
                if (this.clientState == ClientState.UNCONNECTD) {
                    logger.error("RecieverClient: can't connect to available pubserver transmission thread," 
                        + "please contact customer service");
                    break;
                }
                if (!sendConnect()) {
                    logger.error("RecieverClient: send connect message to pubserver failed");
                    break;
                }
                // recv message from pubserver transmission thread
                this.recvMessage();
                
            } catch (InvalidProtocolBufferException ipbe) {
                // reason of this exception is the sdk fail over, then recieve some uncomplete flow data
                if (ipbe.getMessage().equals("Protocol message was too large.  May be malicious.  "
                    + "Use CodedInputStream.setSizeLimit() to increase the size limit.")) {
                    if (invalidProtocolRetry ++ > MAX_TIMES_RETEY) {
                        invalidProtocolRetry = 0;
                        logger.error("RecieverClient: Recieved field size is far too large");
                        break;
                    }
                }
                continue;
            } catch (Exception e) {
                logger.error(e.getMessage());
                // if exception when communication with pubserver transmission thread, retry to connect
                if (!e.getMessage().equals("connect to pubserver transmission thread fail")) {
                    break;
                }
            }
        }
    }
    
    /**
     * 
     * @return real-time consuming offset
     */
    public String getOffset() {
        return this.lastOffset;
    }
    
    /**
     * 
     * @return real-time consuming binlog position
     */
    public String getPosition() {
        return this.lastPosition;
    }
    
    /**
     * 
     * @return real-time consuming timestamp
     */
    public String getTimestamp() {
        return this.lastTimestamp;
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
     * exit reciever client thread
     * @throws Exception 
     */
    public void clientExit() throws Exception {
        logger.debug("RecieverClient: notify reciever client thread to exit");
        this.isExit = true;
        this.join();
        logger.debug("RecieverClient: reciever client thread exits");
    }
    
    /**
     * connect to pubserver transmission thread
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
                if(null != proxy) {
                    socket = new Socket(proxy);
                    socket.connect(new InetSocketAddress(host, port));
                }else {
                    socket = new Socket(host, port);
                }
                output = CodedOutputStream.newInstance(socket.getOutputStream());
                input = CodedInputStream.newInstance(socket.getInputStream());
                logger.debug("RecieverClient: connect to pubServer successfully");
                this.clientState = ClientState.CONNECTED;
                break;
            }
            catch (Exception e) {
                logger.warn("RecieverClient: connect to PubServer fail, errmsg:{}, retry:{}", e.getMessage(), retry);
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
     * send Connect message to Pubserver
     * @return
     */
    private boolean sendConnect() {
        try {
            logger.debug("connect builder");
            Connect connect = Connect.newBuilder()
                    .setType(MsgType.CONNECT)
                    .setHost("")
                    .setPort("")
                    .setDtsId(this.dtsId)
                    .build();
            // send ack message to pubserver
            logger.debug("send ack message to pubserver");
            NetIoHelper.sendMessage(connect, this.output);
            return true;
        }
        catch (Exception e) {
            logger.error("RecieverClient: send Connect message to Pubserver exception: {}", e.getMessage());
        }
        return false;    
    }
    
    private void recvMessage() throws Exception {
        while (!this.isExit) {       
            DataMessage dataMsg = new DataMessage();
            int counterRecord = 0;
            long timeoutMs = 0;
            boolean isEnd = false;

            // when counter of receving event is reach to MAX_EVENT_COUNTER, notify listener to consume
            // when waiting time is reach to MAX_EVENT_TIMEOUT_MS, also notify listener to consume
            /*
            while (counterRecord++ < MAX_EVENT_COUNTER && timeoutMs < MAX_EVENT_TIMEOUT_MS) {
                long beforeTime = System.currentTimeMillis();
                Event event = (Event)NetIoHelper.recieveMessage(MsgType.EVENT_VALUE, this.input);
                offset = event.getOffset();
                position = event.getPosition();
                timestamp = event.getTimestamp();
                if (event.getIsEnd()) {
                    isEnd = true;
                    break;
                }
                Record record = this.convertEventToRecord(event);
                dataMsg.addRecord(record);
                long afterTime = System.currentTimeMillis();
                timeoutMs += afterTime - beforeTime;
            }
            */
            
            // Each DataMessage only contains ONE record
//            /*
            Event event = (Event) NetIoHelper.recieveMessage(MsgType.EVENT_VALUE, this.input);
            Record record = this.convertEventToRecord(event);
            dataMsg.addRecord(record);
            offset = event.getOffset();
            position = event.getPosition();
            timestamp = event.getTimestamp();            
            if (event.getIsEnd()) {
                isEnd = true;
            }
//            */
            
            for (ConsumerListener listener : this.listeners) {
                lastOffset = offset;
                lastPosition = position;
                lastTimestamp = timestamp;
                listener.notify(dataMsg);
            }
            
            if (isEnd) {
                logger.debug("Reciever ends.dts id:{},last offset:{},last binlog position:{},last timestamp:{}", 
                        this.dtsId, this.lastOffset, this.lastPosition, this.lastTimestamp);
            }
            counterRecord = 0;
            timeoutMs = 0;
        }
    }
    
    /**
     * receive event from pubserver transmission thread and convert event to record
     * 
     * @return        DataMessage Record if successfully, or throw exception
     * @throws Exception
     */
    private Record convertEventToRecord(Event event) throws Exception {
        try {
            Record record = new Record();
            record.addAttribute("db", event.getDbName());
            record.addAttribute("table_name", event.getTblName());
            record.setType(Record.Type.valueOf(event.getRecordTypeValue()));
            record.addAttribute("offset", event.getOffset());
            record.addAttribute("position", event.getPosition());
            record.addAttribute("timestamp", event.getTimestamp());
            record.setFieldList(event.getFieldsList());
            record.addAttribute("ddl", event.getDdl());
            return record;
        }
        catch (Exception e) {
            throw new Exception("convertEventToRecord exception: " + e.getMessage());
        }
    }
    
}
