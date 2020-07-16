package org.example.mq.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;


public class TranProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建消息生产者producer，并制定生产者组名
        TransactionMQProducer producer = new TransactionMQProducer("group2");
        //2.指定Nameserver地址,集群地址用逗号隔开
        producer.setNamesrvAddr("192.168.59.131:9876");

        //设置事务监听器
        producer.setTransactionListener(new TransactionListener() {
            /**
             * 生产者在该方法中执行本地的事务
             * @param message
             * @param o
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                if (StringUtils.equals("tag1",message.getTags())){
                    return LocalTransactionState.COMMIT_MESSAGE;
                }else if (StringUtils.equals("tag2",message.getTags())){
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }else if (StringUtils.equals("tag3",message.getTags())){
                    //即不提交事务也不回滚事务，因为询问机制，mq会回查是否可以Commit或者Rollback那些由于错误没有被终结的HalfMsg
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.UNKNOW;
            }
            /**
             * 该方法是mq进行消息事务状态回查
             * @param messageExt
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println("消息的tag:"+messageExt.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        //3.启动producer
        producer.start();
        System.out.println("生产者启动");

        String[] tag={"tag1","tag2","tag3"};
        for (int i = 0; i < 3; i++) {
            //4.创建消息对象，指定主题Topic、Tag和消息体
            /**
             * 参数一：消息主题Topic
             * 参数二：消息Tag
             * 参数三：消息内容
             */
            Message message = new Message("TransactionTopic", tag[i], ("hello world" + i).getBytes());
            //5.发送事务消息
            //第二个参数，在进行事务控制的时候，可以将事务应用到某一个消息上，
            //也可以应用到producer上，该producer发送的消息都会进行事务控制
            //null表示应用到producer
            SendResult send = producer.sendMessageInTransaction(message,null);
            System.out.println(send.toString());
        }
        //6.关闭生产者producer
        //由于mq要回查生产者，所以不要停了生产者
//        producer.shutdown();
    }
}
