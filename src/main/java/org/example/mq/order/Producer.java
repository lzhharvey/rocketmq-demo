package org.example.mq.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName Producer
 * @Description TODO
 * @Author liuzhihui
 * @Date 2020/7/16 14:40
 * @Version 1.0
 **/
public class Producer {
    private static void run() throws MQClientException, InterruptedException, RemotingException, MQBrokerException {
        //1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定Nameserver地址,集群地址用逗号隔开
        producer.setNamesrvAddr("192.168.59.131:9876");
        //3.启动producer
        producer.start();
        //构建订单步骤集合
        List<OrderStep> orderSteps = OrderStep.buildOrders();

        for (int i = 0; i < orderSteps.size(); i++) {
            /**
             * 4.创建消息对象，指定主题Topic、Tag和消息体
             * 参数一：消息主题Topic
             * 参数二：消息Tag
             * 参数三：消息key
             * 参数四：消息内容
             */
            Message message = new Message("OrderTopic", "order", "i"+i,orderSteps.get(i).toString().getBytes());
            /**
             * 5.发送消息
             * 参数一：消息对象
             * 参数二：消息队列选择器
             * 参数三：选择队列的业务标识(订单id)
             */
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                /**
                 * 选择队列
                 * @param list 队列集合 默认四个队列，要修改的话broker配置文件defaultTopicQueueNums=x
                 * @param message 消息对象
                 * @param o 业务表示的参数
                 * @return
                 */
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Long orderId = (Long) o;
                    //取余，选取队列
                    long l = orderId % list.size();
                    return list.get((int) l);
                }
            }, orderSteps.get(i).getOrderId());

            System.out.println("发送结果："+sendResult);
        }
        //6.关闭生产者producer
        producer.shutdown();
    }
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        run();
    }
}
