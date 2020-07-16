package org.example.mq.order;

import java.util.ArrayList;
import java.util.List;

/**
 * 订单的步骤
 */
public  class OrderStep {
   private long orderId;
   private String desc;
   public long getOrderId() {
       return orderId;
   }
   public void setOrderId(long orderId) {
       this.orderId = orderId;
   }
   public String getDesc() {
       return desc;
   }
   public void setDesc(String desc) {
       this.desc = desc;
   }
   @Override
   public String toString() {
       return "OrderStep{" +
           "orderId=" + orderId +
           ", desc='" + desc + '\'' +
           '}';
   }
   /**
    * 生成模拟订单数据 3个订单
    */
   public static List<OrderStep> buildOrders() {
       //1039  ：创建 付款 推送 完成 取余3
       //1065  ：创建 付款 完成   取余1
       //7235  ：创建 付款 完成   取余3
       List<OrderStep> orderList = new ArrayList<OrderStep>();

       OrderStep orderDemo = new OrderStep();
       orderDemo.setOrderId(1039L);
       orderDemo.setDesc("创建");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(1065L);
       orderDemo.setDesc("创建");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(1039L);
       orderDemo.setDesc("付款");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(7235L);
       orderDemo.setDesc("创建");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(1065L);
       orderDemo.setDesc("付款");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(7235L);
       orderDemo.setDesc("付款");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(1065L);
       orderDemo.setDesc("完成");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(1039L);
       orderDemo.setDesc("推送");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(7235L);
       orderDemo.setDesc("完成");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(1039L);
       orderDemo.setDesc("完成");
       orderList.add(orderDemo);

       return orderList;
   }
}