package com.hong.kafka.consumer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * 自定义消息分配器
 *
 * @Author: ZhangDeHong
 * @Describe: TODO
 * @Date Create in  10:40 下午 2020/6/17
 */
public class CustomPartitioner implements Partitioner {

    @Override
    public int partition (String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //  获取所有分区
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        // 分区数
        int numPartitions = partitionInfos.size();
        // 必须传key 否则抛异常
        if (null == keyBytes || !(key instanceof String)) {
            throw new InvalidRecordException("kafka message must have key");
        }
        if (1 == numPartitions) {
            return 0;
        }
        // 最后一个分区
        if (key.equals("name")) {
            return numPartitions - 1;
        }
        // Utils.murmur2(keyBytes) 根据key 获取到hash值
        return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
    }

    /**
     * 关闭分区分配器
     */
    @Override
    public void close () {

    }

    /**
     * 分区分配器配置
     */
    @Override
    public void configure (Map<String, ?> map) {

    }
}
