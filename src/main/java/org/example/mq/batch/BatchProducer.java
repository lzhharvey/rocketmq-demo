package org.example.mq.batch;


import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * 发送同步消息
 */
public class BatchProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定Nameserver地址,集群地址用逗号隔开
        producer.setNamesrvAddr("192.168.59.131:9876");
        //3.启动producer
        producer.start();

        //4.批量发送消息
        List<Message> messageList=new ArrayList<Message>();
        Message message1=new Message("base","tag1",("hello"+1).getBytes());
        Message message2=new Message("base","tag1",("hello"+2).getBytes());
        Message message3=new Message("base","tag1",("hello"+3).getBytes());
        messageList.add(message1);
        messageList.add(message2);
        messageList.add(message3);
        SendResult sendResult = producer.send(messageList);
        System.out.println("发送结果："+sendResult);
        //6.关闭生产者producer
        producer.shutdown();
    }
}
