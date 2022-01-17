package com.nh.springbootrocketmqproducer.entity;

import java.util.ArrayList;
import java.util.List;

public class Order {
    private long id;
    private String desc;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", desc='" + desc + '\'' +
                '}';
    }

    public static  List<Order> buildOrders(){
        List<Order> orderList = new ArrayList<Order>();

        Order o1 = new Order();
        o1.setId(1039L);
        o1.setDesc("创建");
        orderList.add(o1);
        o1 = new Order();
        o1.setId(1039L);
        o1.setDesc("付款");
        orderList.add(o1);
        o1 = new Order();
        o1.setId(1039L);
        o1.setDesc("推送");
        orderList.add(o1);
        o1 = new Order();
        o1.setId(1039L);
        o1.setDesc("完成");
        orderList.add(o1);
        Order o2 = new Order();
        o2.setId(1065L);
        o2.setDesc("创建");
        orderList.add(o2);
        o2 = new Order();
        o2.setId(1065L);
        o2.setDesc("付款");
        orderList.add(o2);
        Order o3 = new Order();
        o3.setId(7235L);
        o3.setDesc("创建");
        orderList.add(o3);
        o3 = new Order();
        o3.setId(7235L);
        o3.setDesc("付款");
        orderList.add(o3);

        return orderList;
    }
}
