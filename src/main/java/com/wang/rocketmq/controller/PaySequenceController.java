package com.wang.rocketmq.controller;

import com.wang.rocketmq.config.RocketMQConfig;
import com.wang.rocketmq.model.ProductOrder;
import com.wang.rocketmq.producer.PayProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;

@RestController
public class PaySequenceController {

    @Autowired
    private PayProducer payProducer;


    @RequestMapping("/api/seq")
    public Object callback() throws Exception {

        List<ProductOrder> list  = ProductOrder.getOrderList();

        for(int i=0; i< list.size(); i++){
            ProductOrder order = list.get(i);
            Message message = new Message(RocketMQConfig.PAY_TOPIC,"pay",
                    order.getOrderId()+"",order.toString().getBytes());

         SendResult sendResult =  payProducer.getProducer().send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Long id = (Long) arg;
                    long index = id % mqs.size();
                    return mqs.get((int)index);
                }
            },order.getOrderId());

         System.out.printf("发送结果=%s, sendResult=%s ,orderId=%s, type=%s\n", sendResult.getSendStatus(), sendResult.toString(),order.getOrderId(),order.getType());

        }
        return new HashMap<>();
    }


}
