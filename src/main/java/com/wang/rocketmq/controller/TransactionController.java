package com.wang.rocketmq.controller;

import com.wang.rocketmq.config.RocketMQConfig;
import com.wang.rocketmq.producer.TransactionProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

/**
 * @Author: wang
 * @Date: 2019/5/21
 * @Description:
 */

@RestController
public class TransactionController {

    @Autowired
    private TransactionProducer transactionMQProducer;


    @RequestMapping("/api/transaction")
    public Object callback( String tag, String otherParam ) throws Exception {

        Message message = new Message(RocketMQConfig.PAY_TOPIC, tag, tag+"_key",tag.getBytes());



        SendResult sendResult =  transactionMQProducer.getProducer().
                sendMessageInTransaction(message, otherParam);


        System.out.printf("发送结果=%s, sendResult=%s \n", sendResult.getSendStatus(), sendResult.toString());

        return new HashMap<>();
    }

}
