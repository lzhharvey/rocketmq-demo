package org.example.mq.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.CountDownLatch;

/**
 * 发送异步消息
 */
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定Nameserver地址,集群地址用逗号隔开
        producer.setNamesrvAddr("192.168.59.131:9876");
        //3.启动producer
        producer.start();
        //使用减法计数器防止主线程结束了，异步线程还没执行完
        final CountDownLatch countDownLatch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            //4.创建消息对象，指定主题Topic、Tag和消息体
            /**
             * 参数一：消息主题Topic
             * 参数二：消息Tag
             * 参数三：消息内容
             */
            Message message = new Message("base", "Tag2", ("hello world" + i).getBytes());
            //5.发送异步消息
            producer.send(message, new SendCallback() {
                /**
                 * 发送成功回调函数
                 */
                public void onSuccess(SendResult sendResult) {
                    //减法计数器减一
                    countDownLatch.countDown();
                    System.out.println("发送结果："+sendResult);
                }
                /**
                 * 发送失败回调函数
                 */
                public void onException(Throwable throwable) {
                    //减法计数器减一
                    countDownLatch.countDown();
                    System.out.println("发送异常："+throwable);
                }
            });

        }
        //延迟主线程的执行时间，避免主线程结束了，异步线程还没执行完，出现No route info of this topic的异常
        //或者使用CountDownLatch解决
//        Thread.sleep(3000);
        //必要的任务执行完，等待计数器归零，才向下执行
        countDownLatch.await();
        //6.关闭生产者producer
        producer.shutdown();
    }
}
