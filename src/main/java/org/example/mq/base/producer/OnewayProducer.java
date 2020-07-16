package org.example.mq.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * 单向发送消息
 */
public class OnewayProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定Nameserver地址,集群地址用逗号隔开
        producer.setNamesrvAddr("192.168.59.131:9876");
        //3.启动producer
        producer.start();
        for (int i = 0; i < 10; i++) {
            //4.创建消息对象，指定主题Topic、Tag和消息体
            /**
             * 参数一：消息主题Topic
             * 参数二：消息Tag
             * 参数三：消息内容
             */
            Message message = new Message("base", "Tag3", ("hello world，单向消息" + i).getBytes());
            //5.发送单向消息，无返回值
            producer.sendOneway(message);
            //线程睡1秒
            TimeUnit.MILLISECONDS.sleep(1);
        }
        //6.关闭生产者producer
        producer.shutdown();
    }
}
