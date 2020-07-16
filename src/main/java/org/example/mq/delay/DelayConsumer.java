package org.example.mq.delay;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @ClassName DatchConsumer
 * @Description TODO
 * @Author liuzhihui
 * @Date 2020/7/16 16:50
 * @Version 1.0
 **/
public class DelayConsumer {
    private  static void  run() throws MQClientException {
        //1.创建消费者Consumer，制定消费者组名 推模式Broker将消息推给消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //2.指定Nameserver地址
        consumer.setNamesrvAddr("192.168.59.131:9876");
        //3.订阅主题Topic和Tag
        consumer.subscribe("DelayTopic","tag1");
        //4.设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            //接受消息内容
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.println("消息ID:"+messageExt.getMsgId()+" 延迟时间："+(System.currentTimeMillis()-messageExt.getStoreTimestamp()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //5.启动消费者consumer
        consumer.start();
        System.out.println("消费者启动");
    }
    public static void main(String[] args) throws MQClientException {
        run();
    }
}
