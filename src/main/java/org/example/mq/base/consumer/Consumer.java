package org.example.mq.base.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * 消息的接受者
 * 1.创建消费者Consumer，制定消费者组名
 * 2.指定Nameserver地址
 * 3.订阅主题Topic和Tag
 * 4.设置回调函数，处理消息
 * 5.启动消费者consumer
 */
public class Consumer {
    private  static void  run() throws MQClientException {
        //1.创建消费者Consumer，制定消费者组名 推模式Broker将消息推给消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //2.指定Nameserver地址
        consumer.setNamesrvAddr("192.168.59.131:9876");
        //3.订阅主题Topic和Tag
        consumer.subscribe("base","Tag1");
        //4.设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            //接受消息内容
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.println(new String(messageExt.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //5.启动消费者consumer
        consumer.start();
    }

    /**
     * 广播模式
     */
    private  static void  run1() throws MQClientException {
        //1.创建消费者Consumer，制定消费者组名 推模式Broker将消息推给消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //2.指定Nameserver地址
        consumer.setNamesrvAddr("192.168.59.131:9876");
        //3.订阅主题Topic和Tag
        consumer.subscribe("base","Tag1");

        //设置消费模式：广播模式
        consumer.setMessageModel(MessageModel.BROADCASTING);

        //4.设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            //接受消息内容
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.println(new String(messageExt.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //5.启动消费者consumer
        consumer.start();
    }
    public static void main(String[] args) throws MQClientException {
//        run();
        //广播模式
        run1();
    }
}
