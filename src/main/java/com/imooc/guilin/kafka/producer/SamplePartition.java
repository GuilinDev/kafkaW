package com.imooc.guilin.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义Kafka Producer发送消息到broker的过程中的分区器(custom partitioner)
 */
public class SamplePartition implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /**
         * 根据业务来定义什么消息record可以进入分区，
         * 例如key长如下摸样，按照对2取余来计算
         * key-1
         * key-2
         * key-3
         */
        String keyStr = key + "";
        String keyInt = keyStr.substring(4); // 截取key中最后一位的数字
        System.out.println("keyStr: " + keyStr + "keyInt: " + keyInt);

        int i = Integer.parseInt(keyInt);

        return i % 2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
