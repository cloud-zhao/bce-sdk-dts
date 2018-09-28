package com.bce.dts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bce.dts.consumer.ConsumerClient;
import com.bce.dts.consumer.ConsumerClientImpl;
import com.bce.dts.consumer.ConsumerListener;
import com.bce.dts.consumer.RegionContext;
import com.bce.dts.model.DataMessage;
import com.bce.dts.model.Record;

/**
 * class description
 * 
 * @author yushaozai@baidu.com
 * @date 2017-08-10
 * @version 1.0
 */
public class SdkDemo {
    private static final Logger logger = LoggerFactory.getLogger(SdkDemo.class);         

    /**
     * main function
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 创建一个RegionContext
        RegionContext context = new RegionContext();
        // 配置云账号的AccessKey及AccessKeySecret
        context.setAccessKey("4d5743bfdaa84c19bc3f5ae352417311");
        context.setSecretKey("ad36a5bb35a346e4959e253c6719d5fd");
         // 运行SDK的服务器是否使用公网IP连接DTS订阅通道
        context.setUsePublicIp(true);
        
        // 初始化ConsumerClient
        final ConsumerClient client = new ConsumerClientImpl(context);
        
        // 初始化ConsumerListener
        ConsumerListener listener = new ConsumerListener() {
            @Override
            public void notify(DataMessage message) throws Exception {
                for (Record record : message.getRecordList()) {  
                    // 打印订阅到的增量数据
                    System.out.println("---------data-------------");
                    System.out.println(record.toString());
                    System.out.println(record.getDbname() + ":" + record.getTablename() + ":"
                            + record.getOpt());  
                    // 消费完数据后向DTS汇报ACK，必须调用
//                    message.ackAsConsumed();
                    
                }
            }
            @Override
            public void noException(Exception e) {
                e.printStackTrace();
            }
        };
        
        // 添加监听者
        client.addConcurrentListener(listener);
        // 设置请求的订阅通道ID
        client.askForDtsId("dtsmly45zaomlu1dj9kc");
        // 启动后台线程， 注意这里不会阻塞， 主线程不能退出
        client.start();
        Thread.sleep(5000);
        client.stop();
        logger.debug("main thread exists");
    }

}
