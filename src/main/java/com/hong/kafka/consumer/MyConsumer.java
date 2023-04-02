package com.hong.kafka.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @Author: ZhangDeHong
 * @Describe: TODO
 * @Date Create in  10:07 下午 2020/7/6
 */
public class MyConsumer {

    private static KafkaConsumer<String, String> consumer;
    private static final Properties properties;

    static {
        properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 指定消费者组
        properties.put("group.id", "kafkaStudy");
    }

    /**
     * 自动提交位移的方式消费数据
     */
    private static void generalConsumeMessageAutoCommit () {
        // 允许自动提交位移
        properties.put("enable.auto.commit", true);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("hong-kafka-study"));
        try {
            while (true) {
                boolean flag = true;
                // 拉取数据
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic = %s , partition = %s , key = %s , value = %s%n",

                            record.topic(), record.partition(), record.key(), record.value()
                    );
                    if (record.value().equals("done")) {
                        flag = false;
                    }
                }
                if (!flag) {
                    break;
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 同步提交
     */
    private static void generalConsumeMessageSyncCommit () {
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("hong-kafka-study"));
        while (true) {
            boolean flag = true;
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s , partition = %s , key = %s , value = %s%n",

                        record.topic(), record.partition(), record.key(), record.value()
                );
                if (record.value().equals("done")) {
                    flag = false;
                }
            }
            try {
                // 发起应用提交  会阻塞 ，减少手动提交的频率，会增加消息重复的概率
                // 提交失败会重试，直到提交成功 或者 抛出异常 兼容失败的过程
                consumer.commitSync();
            } catch (CommitFailedException e) {
                System.out.println("commit fail error ：" + e.getMessage());
            }
            if (!flag) {
                break;
            }
        }
    }

    /**
     * 异步提交
     */
    private static void generalConsumeMessageAsyncCommit () {
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("hong-kafka-study"));
        while (true) {
            boolean flag = true;
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s , partition = %s , key = %s , value = %s%n",

                        record.topic(), record.partition(), record.key(), record.value()
                );
                if (record.value().equals("done")) {
                    flag = false;
                }
            }
            try {
                // Commit A offset 2000
                // Commit B offset 3000  如果提交失败，不会重试
                // 会有重复消费消息
                consumer.commitAsync();
            } catch (CommitFailedException e) {
                System.out.println("commit fail error ：" + e.getMessage());
            }
            if (!flag) {
                break;
            }
        }
    }

    private static void generalConsumeMessageAsyncCommitWithCallback () {
        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("hong-kafka-study"));
        while (true) {
            boolean flag = true;
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s , partition = %s , key = %s , value = %s%n",
                        record.topic(), record.partition(), record.key(), record.value()
                );
                if (record.value().equals("done")) {
                    flag = false;
                }
            }
            //consumer.commitAsync(new OffsetCommitCallback() {
            //    @Override
            //    public void onComplete (Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
            //        if (null != e) {
            //            System.out.println("commit fail for offset ："+e.getMessage());
            //        }
            //    }
            //});
            consumer.commitAsync((map, e) -> {
                if (null != e) {
                    System.out.println("commit fail for offset ：" + e.getMessage());
                }
            });
            if (!flag) {
                break;
            }
        }
    }

    private static void mixSyncAndAsyncCommit () {
        properties.put("enable.auto.commit", true);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("hong-kafka-study"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic = %s , partition = %s , key = %s , value = %s%n",
                            record.topic(), record.partition(), record.key(), record.value()
                    );
                }
                consumer.commitAsync();
            }
        } catch (Exception e) {
            System.out.println("commit async error :" + e.getMessage());
        } finally {
            try {
                consumer.commitSync();
            }catch (CommitFailedException e){
                consumer.close();
            }
        }
    }

    public static void main (String[] args) {
        generalConsumeMessageAutoCommit();
    }
}
