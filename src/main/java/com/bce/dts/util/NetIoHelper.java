package com.bce.dts.util;

import java.io.IOException;
import java.io.Serializable;

import com.bce.dts.protobuf.AckOuterClass.Ack;
import com.bce.dts.protobuf.AuthOuterClass.Auth;
import com.bce.dts.protobuf.Common.MsgType;
import com.bce.dts.protobuf.ConnectOuterClass.Connect;
import com.bce.dts.protobuf.EndOuterClass.End;
import com.bce.dts.protobuf.EventOuterClass.Event;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetIoHelper {
    /**
     * message magic header
     */
    private static final int MAGIC_HEADER = 0xdeadbabe;
    /**
     * publish server port
     */
    public static final int PUBSERVER_PORT = 8765;

    private static final Logger logger = LoggerFactory.getLogger(NetIoHelper.class);

    /**
     * check magic header of recieved message
     * @param codedInput
     * @return
     * @throws  IOException, InterruptedException 
     */
    public static boolean checkMagicHeader(CodedInputStream codedInput) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        while (codedInput.isAtEnd()) {
            Thread.sleep(5000);
        }
        int comMagicHeader = NetIoHelper.reverseEvery8Bit(codedInput.readFixed32());
        if (comMagicHeader != MAGIC_HEADER) {
            return false;
        }
        return true;
    }

    /**
     * send message(auth) to output stream
     * @param auth
     * @throws Exception
     */
    public static void sendMessage(Auth auth, CodedOutputStream codedOutput) throws IOException {
        // TODO Auto-generated method stub
        final int serialized = auth.getSerializedSize();
        codedOutput.writeFixed32NoTag(MAGIC_HEADER);
        codedOutput.writeFixed32NoTag(serialized);
        auth.writeTo(codedOutput);
        codedOutput.flush();
    }
    
    /**
     * send message(ack) to output stream
     * @param ack
     * @throws Exception
     */
    public static void sendMessage(Ack ack, CodedOutputStream codedOutput) throws IOException {
        // TODO Auto-generated method stub
        final int serialized = ack.getSerializedSize();
        codedOutput.writeFixed32NoTag(MAGIC_HEADER);
        codedOutput.writeFixed32NoTag(serialized);
        ack.writeTo(codedOutput);
        codedOutput.flush();
    }
    
    /**
     * send message(connect) to output stream
     * @param connect
     * @throws Exception
     */
    public static void sendMessage(Connect connect, CodedOutputStream codedOutput) throws IOException {
        // TODO Auto-generated method stub

        logger.debug("send message connect output");
        final int serialized = connect.getSerializedSize();
        logger.debug("codeoutput " + MAGIC_HEADER);
        codedOutput.writeFixed32NoTag(MAGIC_HEADER);
        logger.debug("codeOutput " + serialized);
        codedOutput.writeFixed32NoTag(serialized);
        logger.debug("connect write codeOutput " + connect.toString());
        connect.writeTo(codedOutput);
        logger.debug("codeOutput flush");
        codedOutput.flush();
    }
    
    /**
     * Receive message from publish server
     * @param msgType
     * @return
     * @throws Exception
     */
    public static Serializable recieveMessage(int msgType, CodedInputStream codedInput) throws Exception {
        // TODO Auto-generated method stub
        if (!NetIoHelper.checkMagicHeader(codedInput)) {
            throw new Exception("Recieved message is invalid.");
        }
        
        int messageLength = NetIoHelper.reverseEvery8Bit(codedInput.readFixed32());
        Serializable ret = null;
        byte[] content = codedInput.readRawBytes(messageLength);
        switch (msgType) {
            case MsgType.ACK_VALUE:
                ret = Ack.parseFrom(content);
                break;
            case MsgType.END_VALUE:
                ret = End.parseFrom(content);
                break;
            case MsgType.EVENT_VALUE:
                ret = Event.parseFrom(content);
                break;
            case MsgType.UNKNOW_VALUE:
                break;
    
            default:
                break;
        }
        return ret;
    }

    /**
     * reverse integer every 8 bits
     * @param input
     * @return
     */
    public static int reverseEvery8Bit(int input) {
        int y = 0;
        int num = input;
        while (num != 0) {
            y <<= 8;
            y = y | num & 255;
            num >>>= 8;
        }
        return y;
    }
}
