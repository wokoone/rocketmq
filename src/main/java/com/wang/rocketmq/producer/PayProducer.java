package com.wang.rocketmq.producer;

import com.wang.rocketmq.config.RocketMQConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.stereotype.Component;

/**
 * @Author: wang
 * @Date: 2019/5/19 15:51
 * @Description:
 */

@Component
public class PayProducer {


    private DefaultMQProducer producer;

    public PayProducer(){
        producer = new DefaultMQProducer(RocketMQConfig.PAY_GROUP);
        //指定NameServer地址，多个地址以 ; 隔开
        //如 producer.setNamesrvAddr("192.168.100.141:9876;192.168.100.142:9876;192.168.100.149:9876");
        producer.setNamesrvAddr(RocketMQConfig.NAME_SERVER);
        start();
    }

    public DefaultMQProducer getProducer(){
        return this.producer;
    }

    /**
     * 对象在使用之前必须要调用一次，只能初始化一次
     */
    private void start(){
        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 一般在应用上下文，使用上下文监听器，进行关闭
     */
    public void shutdown(){
        this.producer.shutdown();
    }


}

