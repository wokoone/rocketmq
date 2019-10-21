package com.wang.rocketmq.controller;

import com.wang.rocketmq.config.RocketMQConfig;
import com.wang.rocketmq.producer.PayProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

@RestController
public class PayController {

    @Autowired
    private PayProducer payProducer;


    @RequestMapping("/api/test")
    public Object callback(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        Message message = new Message(RocketMQConfig.PAY_TOPIC,"pay","1", text.getBytes() );

        SendResult sendResult = payProducer.getProducer().send(message);
        System.out.println("发送消息结果：" + sendResult);

        return new HashMap<>();
    }

}
