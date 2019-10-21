package com.wang.rocketmq.consumer;

import com.wang.rocketmq.config.RocketMQConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;

@Component
public class PayConsumer {


    private DefaultMQPushConsumer consumer;


    public  PayConsumer() throws MQClientException {

        consumer = new DefaultMQPushConsumer(RocketMQConfig.PAY_GROUP);
        consumer.setNamesrvAddr(RocketMQConfig.NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        consumer.subscribe(RocketMQConfig.PAY_TOPIC, "*");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            MessageExt msg = msgs.get(0);
            int times = msg.getReconsumeTimes();
            System.out.println("重试次数 = " + times);
            try {
                String topic = msg.getTopic();
                String body = new String(msg.getBody(), "utf-8");
                String tags = msg.getTags();
                String keys = msg.getKeys();
                System.out.println("消费者收到消息：topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (UnsupportedEncodingException e) {
                System.out.println("消费异常");
                // 如果默认重试2次不成功，则记录，人工补偿
                if(times >= 2){
                    System.out.println("重试次数大于2，记录数据库，短信通知开发人员或者运营人员人工补偿");
                    //TODO 记录数据库，短信通知开发人员或者运营人员人工补偿
                    //告诉broker，消息消费成功，不再重试
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        consumer.start();
        System.out.println("consumer start ...");
    }

}
