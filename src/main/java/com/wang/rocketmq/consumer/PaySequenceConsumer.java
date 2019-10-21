package com.wang.rocketmq.consumer;

import com.wang.rocketmq.config.RocketMQConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;

import java.util.List;

//@Component
public class PaySequenceConsumer {


    private DefaultMQPushConsumer consumer;


    public PaySequenceConsumer() throws MQClientException {

        consumer = new DefaultMQPushConsumer(RocketMQConfig.PAY_GROUP);
        consumer.setNamesrvAddr(RocketMQConfig.NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        //默认是集群方式，可以更改为广播，但是广播方式不支持重试
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.subscribe(RocketMQConfig.PAY_TOPIC, "*");


        consumer.registerMessageListener( new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                try {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));

                //做业务逻辑操作 TODO

                return ConsumeOrderlyStatus.SUCCESS;

            } catch (Exception e) {

                e.printStackTrace();
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
            }

        });

        consumer.start();
        System.out.println("consumer start ...");
    }

}
