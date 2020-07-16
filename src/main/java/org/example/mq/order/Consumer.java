package org.example.mq.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @ClassName Consumer
 * @Description TODO
 * @Author liuzhihui
 * @Date 2020/7/16 15:12
 * @Version 1.0
 **/
public class Consumer {
    private  static void  run() throws MQClientException {
        //1.创建消费者Consumer，制定消费者组名 推模式Broker将消息推给消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //2.指定Nameserver地址
        consumer.setNamesrvAddr("192.168.59.131:9876");

        //3.订阅主题Topic和Tag  *表示这个主题下的所有消息都消费
        consumer.subscribe("OrderTopic","*");
        //4.设置回调函数，处理消息
        //MessageListenerOrderly在进行消息消费的时候，对于一个队列的消息用一个线程处理
        consumer.registerMessageListener(new MessageListenerOrderly(){
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.println("线程名："+Thread.currentThread().getName()+"消费消息："+new String(messageExt.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        //5.启动消费者consumer
        consumer.start();
        System.out.println("消费者启动---------------------------");
    }
    public static void main(String[] args) throws MQClientException {
        run();
    }
}
//消费者启动---------------------------
//        线程名：ConsumeMessageThread_1消费消息：OrderStep{orderId=1065, desc='创建'}
//        线程名：ConsumeMessageThread_2消费消息：OrderStep{orderId=1039, desc='创建'}
//        线程名：ConsumeMessageThread_2消费消息：OrderStep{orderId=1039, desc='付款'}
//        线程名：ConsumeMessageThread_1消费消息：OrderStep{orderId=1065, desc='付款'}
//        线程名：ConsumeMessageThread_2消费消息：OrderStep{orderId=7235, desc='创建'}
//        线程名：ConsumeMessageThread_1消费消息：OrderStep{orderId=1065, desc='完成'}
//        线程名：ConsumeMessageThread_2消费消息：OrderStep{orderId=7235, desc='付款'}
//        线程名：ConsumeMessageThread_2消费消息：OrderStep{orderId=1039, desc='推送'}
//        线程名：ConsumeMessageThread_2消费消息：OrderStep{orderId=7235, desc='完成'}
//        线程名：ConsumeMessageThread_2消费消息：OrderStep{orderId=1039, desc='完成'}