package com.hong.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * kafka有三种发送消息的方式
 *
 * @Author: ZhangDeHong
 * @Describe: TODO
 * @Date Create in  11:10 下午 2020/6/15
 */
public class MyProducer {

    private static final KafkaProducer<String, String> producer;

    static {
        Properties properties = new Properties();
        // 9092 kafka启动占用的默认端口
        // host和port的broker列表  生产者建立连接后会从相应的broker获取集群的信息 建议至少两个，一个宕机后另一个可以继续工作
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        // kafka只接受字节数组，生产者的发送消息接口容许发送任何的Java对象，需要将对象序列化为字节数组
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class", "com.hong.kafka.consumer.CustomPartitioner");
        producer = new KafkaProducer<>(properties);
    }

    // 只发不管结果
    private static void sendMessageForgetResult () {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "hong-kafka-study", "name", "ForgetResult"
        );
        producer.send(record);
        producer.close();
    }

    // 同步发送消息
    public static void sendMessageSync () throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "hong-kafka-study", "name", "syncMessage"
        );
        // 返回发送消息的源信息
        RecordMetadata result = producer.send(record).get();
        System.out.println(result.topic());
        // 发送到的分区
        System.out.println(result.partition());
        // 偏移
        System.out.println(result.offset());
        producer.close();
    }

    // 异步发送消息
    private static void sendMessageCallback () {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "hong-kafka-study", "name", "callBackMessage"
        );
        producer.send(record, new MyProducerCallBack());
        producer.close();
    }

    private static class MyProducerCallBack implements Callback {

        @Override
        public void onCompletion (RecordMetadata recordMetadata, Exception e) {
            if (null != e) {
                e.getStackTrace();
                return;
            }
            System.out.println(recordMetadata.topic());
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
            System.out.println("Coming in MyProducerCallBack");
        }
    }

    public static void main (String[] args) throws Exception {
        // sendMessageForgetResult();
        // sendMessageSync();
        sendMessageCallback();
    }
}
