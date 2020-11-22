package com.imooc.guilin.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerSample {

    private static final String TOPIC_NAME = "gz_topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Producer异步发送演示
//        producerSend();
        //Producer异步阻塞（同步）发送
//        producerSyncSend();
        //Producer异步发送带回调演示
//        producerSendWithCallback();
        //Producer异步发送带回调演示 + 自定义custom partitioner负载均衡
        producerSendWithCallbackAndCustomPartitioner();
    }
    /**
     * Producer 异步发送模式
     */
    public static void producerSend() {
        Properties properties = new Properties(); //基础配置内容
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.252:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象 ProducerRecoder
        for (int i = 0; i < 10; i++) { // 模拟一次发十条不同的消息record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            producer.send(record);
        }

        // kafka所有的通道打开后都需要关闭
        producer.close();
    }

    /**
     * Producer 同步异步阻塞发送（同步发送)
     * 跟异步发送的区别仅在于send()有返回值，可以查看和打印返回值
     */
    public static void producerSyncSend() throws ExecutionException, InterruptedException {
        Properties properties = new Properties(); //基础配置内容
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.252:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象 ProducerRecoder
        for (int i = 0; i < 10; i++) { // 模拟一次发十条不同的消息record
            String key = "key-" + i;
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, key, "value-" + i);
            Future<RecordMetadata> send = producer.send(record);
            RecordMetadata recordMetadata = send.get();
            System.out.println(key +
                    " partition: " + recordMetadata.partition() +
                    " , offset: " + recordMetadata.offset());
        }

        // kafka所有的通道打开后都需要关闭
        producer.close();
    }

    /**
     * Producer 异步发送带回调callback模式
     */
    public static void producerSendWithCallback() {
        Properties properties = new Properties(); //基础配置内容
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.252:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象 ProducerRecoder
        for (int i = 0; i < 10; i++) { // 模拟一次发十条不同的消息record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println(
                            " partition: " + recordMetadata.partition() +
                            " , offset: " + recordMetadata.offset());
                }
            });
        }

        // kafka所有的通道打开后都需要关闭
        producer.close();
    }

    /**
     * Producer 异步发送带回调callback模式 + 自定义分区器custom partitioner负载均衡
     */
    public static void producerSendWithCallbackAndCustomPartitioner() {
        Properties properties = new Properties(); //基础配置内容
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.252:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.imooc.guilin.kafka.producer.SamplePartition"); //自定义分区器

        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象 ProducerRecoder
        for (int i = 0; i < 10; i++) { // 模拟一次发十条不同的消息record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // custom partitioner，比如2个分区，对2取余，最后打印的结果即消息平均去两个分区
                    System.out.println(
                            " partition: " + recordMetadata.partition() +
                                    " , offset: " + recordMetadata.offset());
                }
            });
        }

        // kafka所有的通道打开后都需要关闭
        producer.close();
    }
}

