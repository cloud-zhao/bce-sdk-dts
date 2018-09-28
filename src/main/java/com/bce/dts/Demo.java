package com.bce.dts;

import com.bce.dts.consumer.ConsumerClient;
import com.bce.dts.consumer.ConsumerClientImpl;
import com.bce.dts.consumer.ConsumerListener;
import com.bce.dts.consumer.RegionContext;
import com.bce.dts.model.DataMessage;
public class Demo {
    public static void main(String[] args) {
        // Initialize user identity
        String akString = "填入您的Access Key ID";
        String skString = "填入您的Secret Access Key";
        String dtsId = "填入您在DTS控制台发布的数据订阅任务ID";
         
        // Initialize RegionContext
        RegionContext regionContext = new RegionContext();
        regionContext.setUsePublicIp(false);
        regionContext.setAccessKey(akString);
        regionContext.setSecretKey(skString);
         
        ConsumerClient consumerClient = new ConsumerClientImpl(regionContext);
        consumerClient.addConcurrentListener(new ConsumerListener() {  
            @Override
            public void notify(DataMessage dataMsg) throws Exception {
                System.out.println(dataMsg);
            }
             
            @Override
            public void noException(Exception paramException) {
                 
            }
        });
         
        try {
            consumerClient.askForDtsId(dtsId);
            consumerClient.askForUserId("");
            consumerClient.start();   
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
}