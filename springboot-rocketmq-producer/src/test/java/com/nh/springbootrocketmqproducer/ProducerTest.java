package com.nh.springbootrocketmqproducer;

import com.nh.springbootrocketmqproducer.entity.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest(classes={SpringbootRocketmqProducerApplication.class})
@Slf4j
public class ProducerTest {


    @Value("${rocketmq.name-server}")
    private String NAMESERVER_ADDR;
    @Value("${rocketmq.producer.group}")
    private String PRODUCER_GROUP;
    @Value("${rocketmq.consumer.group}")
    private String CONSUMER_GROUP;

    /**
     * 同步消息发送
     * 同步消息发送是指，Producer发出消息后，需要收到MQ返回ACK之后才会发送下一条消息，该方
     * 式的消息可靠性最高，效率低
     */
    @Test
    public void syncProducer()throws Exception{
        // 实例化消息生产者
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setInstanceName(System.currentTimeMillis()+"");
        //设置NameServer的地址
        producer.setNamesrvAddr(NAMESERVER_ADDR);
        //启动Producer实例
        producer.start();
        for(int i=0;i<100;i++){
            Message msg = new Message("baseTopic","TagA",("Hello RocketMQ"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n",sendResult);
        }
        //关闭Producer实例
        producer.shutdown();
    }

    /**
     * 异步消息发送
     * 异步发送消息是指，Producer发出消息后无需等待MQ返回ACK，直接发送下一条消息，该方式消
     * 息可靠性有保障，效率也可以
     */
    @Test
    public  void asyncProducer()throws Exception{
        // 实例化消息生产者
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setInstanceName(System.currentTimeMillis()+"");
        //设置NameServer的地址
        producer.setNamesrvAddr(NAMESERVER_ADDR);
        //启动Producer实例
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        for(int i=0;i<100;i++){
            Message msg = new Message("asyncTopic","TagB",("Hello RocketMQ"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(msg, new SendCallback() {
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%s%n",sendResult);
                }

                public void onException(Throwable throwable) {
                    System.out.println(throwable.toString());
                }
            });
        }
        //异步shutdown必须有等待，不然等不到回调信息
        TimeUnit.SECONDS.sleep(5000);
        //关闭Producer实例
        producer.shutdown();
    }

    /**
     * 单向消息发送
     * 单向发送是指，Producer只负责发消息，不等待，不处理MQ的ACK，MQ也不会返回ACK，该方
     * 式消息可靠性最低，效率最高
     */
    @Test
    public void oneWayProducer()throws Exception{
        // 实例化消息生产者
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setInstanceName(System.currentTimeMillis()+"");
        //设置NameServer的地址
        producer.setNamesrvAddr(NAMESERVER_ADDR);
        //启动Producer实例
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        for(int i=0;i<100;i++){
            Message msg = new Message("oneWayTopic","TagC",("Hello RocketMQ 单向消息"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.sendOneway(msg);
        }
        //关闭Producer实例
        producer.shutdown();
    }

    @Test
    public void produceMsg() throws  Exception{
        // 实例化消息生产者
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setInstanceName(System.currentTimeMillis()+"");
        //设置NameServer的地址
        producer.setNamesrvAddr(NAMESERVER_ADDR);
        //启动Producer实例
        producer.start();
        List<Order> orders = Order.buildOrders();
        for(int i=0;i<orders.size();i++){
            Message msg = new Message("OrderTopic","Order","i"+i, orders.get(i).toString().getBytes());
            SendResult send = producer.send(msg, new MessageQueueSelector() {
                // 将同一个业务逻辑放在同一个队列中，这里使用orderId作为区分
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    long orderId = (Long) o;
                    int index = (int) orderId % list.size();
                    return list.get(index);
                }
            }, orders.get(i).getId());
            System.out.println("发送结果："+send);
        }
        //关闭Producer实例
        producer.shutdown();
    }

    /**
     * 基本消息消费,//默认是负载均衡模式
     * @throws Exception
     */
    @Test
    public  void basicConsumer()throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESERVER_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("OrderTopic","*");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                consumeOrderlyContext.setAutoCommit(true);
                for(MessageExt msg:list){
                    System.out.println(Thread.currentThread().getName()+"消费消息："+new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        TimeUnit.SECONDS.sleep(20000);
    }

    /**
     * 延迟消息发送
     */
    @Test
    public void delayProducer()throws Exception{
        // 实例化消息生产者
        DefaultMQProducer producer = new DefaultMQProducer(CONSUMER_GROUP);
        producer.setInstanceName(System.currentTimeMillis()+"");
        //设置NameServer的地址
        producer.setNamesrvAddr(NAMESERVER_ADDR);
        //启动Producer实例
        producer.start();
        for(int i=0;i<100;i++){
            Message msg = new Message("DelayTopic","TagD",("Hello RocketMQ"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
            msg.setDelayTimeLevel(2);
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n",sendResult);
        }
        //关闭Producer实例
        producer.shutdown();
    }

    /**
     * 基本消息消费,//默认是负载均衡模式
     * @throws Exception
     */
    @Test
    public void delayConsumer() throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESERVER_ADDR);
        consumer.subscribe("DelayTopic","*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            // 接收消息内容
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for(MessageExt msg:list){
                    System.out.println("消息ID:"+msg.getMsgId()+",延迟时间:"+(System.currentTimeMillis()-msg.getStoreTimestamp()));
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        TimeUnit.SECONDS.sleep(20000);
    }

    /**
     * 批量消息发送
     */
    @Test
    public void batchProducer()throws Exception{
        // 实例化消息生产者
        DefaultMQProducer producer = new DefaultMQProducer(CONSUMER_GROUP);
        producer.setInstanceName(System.currentTimeMillis()+"");
        //设置NameServer的地址
        producer.setNamesrvAddr(NAMESERVER_ADDR);
        //启动Producer实例
        producer.start();
        List<Message> list = new ArrayList<Message>();
        for(int i=0;i<100;i++){
            Message msg = new Message("BatchTopic","Tag1",("Hello RocketMQ"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            list.add(msg);
        }
        // 消息不能超过4M
        producer.send(list);
        //关闭Producer实例
        producer.shutdown();
    }

    /**
     * 批量消息消费
     * @throws Exception
     */
    @Test
    public  void batchConsumer()throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESERVER_ADDR);
        consumer.subscribe("BatchTopic","*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            // 接收消息内容
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for(MessageExt msg:list){
                    System.out.println("消息ID:"+msg.getMsgId());
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        TimeUnit.SECONDS.sleep(20000);
    }
    /**
     * 过滤消息
     */
    @Test
    public  void filterProducer()throws Exception{
        // 实例化消息生产者
        DefaultMQProducer producer = new DefaultMQProducer(CONSUMER_GROUP);

        producer.setInstanceName(System.currentTimeMillis()+"");
        //设置NameServer的地址
        producer.setNamesrvAddr(NAMESERVER_ADDR);
        //启动Producer实例
        producer.start();
        for(int i=0;i<100;i++){
            Message msg = new Message("FilterSQLTopic","Tag1",("Hello RocketMQ"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            msg.putUserProperty("i",String.valueOf(i));
            producer.send(msg);
        }

        //关闭Producer实例
        producer.shutdown();
    }

    /**
     * 通过tag实现过滤
     * @throws Exception
     */
    @Test
    public  void tagFilter()throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESERVER_ADDR);
        // tag过滤
        consumer.subscribe("FilterSQLTopic","Tag1 || Tag2");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            // 接收消息内容
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for(MessageExt msg:list){
                    System.out.println("消息ID:"+msg.getMsgId());
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        TimeUnit.SECONDS.sleep(20000);
    }

    /**
     * 通过sql实现过滤
     * @throws Exception
     */
    @Test
    public  void sqlFilter()throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESERVER_ADDR);
        // tag过滤
        // 需要确认broker的配置文件中是否有该项 enablePropertyFilter = true
        consumer.subscribe("FilterSQLTopic", MessageSelector.bySql("i>90"));
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            // 接收消息内容
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for(MessageExt msg:list){
                    System.out.println("消息ID:"+msg.getMsgId());
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        TimeUnit.SECONDS.sleep(20000);
    }

    /**
     * 事务消息发送
     */
    @Test
    public  void transactionProducer()throws Exception{
        // 实例化消息生产者
        TransactionMQProducer producer = new TransactionMQProducer(CONSUMER_GROUP);
        //设置NameServer的地址
        producer.setInstanceName(System.currentTimeMillis()+"");
        producer.setNamesrvAddr(NAMESERVER_ADDR);
        producer.setTransactionListener(new TransactionListener() {
            /**
             * 在该方法中执行本地事务
             * @param message
             * @param o
             * @return
             */
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                if(StringUtils.equals("TAGA",message.getTags())){
                    // 本地提交
                    return LocalTransactionState.COMMIT_MESSAGE;
                }else  if(StringUtils.equals("TAGB",message.getTags())){
                    // 本地回滚
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }else{
                    return LocalTransactionState.UNKNOW;
                }
            }

            /**
             * 该方法是mq进行消息事务状态的回查，
             * 如果本地事务状态不是回滚或提交，则会去调用该方法，反复查，一般查15次
             * @param messageExt
             * @return
             */
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println("消息的Tag:"+messageExt.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        //启动Producer实例
        producer.start();
        for(int i=0;i<100;i++){
            Message msg = new Message("TransactionTopic","TAGB",("Hello RocketMQ"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.sendMessageInTransaction(msg,null);
        }

        //关闭Producer实例,由于有回查，所以不能停
        TimeUnit.SECONDS.sleep(20000);
        // producer.shutdown();
    }

    /**
     * 基本消息消费,//默认是负载均衡模式
     * @throws Exception
     */
    @Test
    public  void transactionConsumer()throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESERVER_ADDR);
        consumer.subscribe("TransactionTopic","*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            // 接收消息内容
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for(MessageExt msg:list){
                    System.out.println("消息ID:"+msg.getMsgId());
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        TimeUnit.SECONDS.sleep(10000000);
    }
}
