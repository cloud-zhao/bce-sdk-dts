package com.bce.dts.protobuf;

import java.util.Arrays;

import com.bce.dts.protobuf.AuthOuterClass.Auth;
import com.bce.dts.protobuf.Common.MsgType;
import com.google.protobuf.InvalidProtocolBufferException;

import junit.framework.TestCase;

public class ProtobufTest extends TestCase {
    public void testA() {
        
    }
    public void testAuthProto() {
        // 构建一个Person对象
        Auth auth = Auth
                .newBuilder()
                .setHost("host")
                .setBceDate("x-bce-date")
                .setAuthorization("authorization")
                .setPath("path")
                .setDtsId("dtsId")
                .setType(MsgType.AUTH)
                .build();
        System.out.println("打印输出Auth对象信息：");
        System.out.println(auth);
        System.out.println("Auth对象调用toString()方法：");
        System.out.println(auth.toString());
    
        System.out.println("Person对象字段是否初始化：" + auth.isInitialized());
    
        // 序列号
        System.out.println("Auth对象调用toByteString()方法：");
        System.out.println(auth.toByteString());
    
        System.out.println("Auth对象调用toByteArray()方法:");
        System.out.println(Arrays.toString(auth.toByteArray()));
         
        try {
            System.out.println("反序列化后的对象信息：");
            // 反序列化
            Auth newAuth = Auth.parseFrom(auth.toByteArray());
            System.out.println(newAuth);
            newAuth = Auth.parseFrom(auth.toByteString());
            System.out.println(newAuth);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
    
}
