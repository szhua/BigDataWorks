package com.szhua.kafkawork.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerTest {

    public static void main(String[] args) {
        String topicName = "flume-kafka";
        Properties props = new Properties();
        props.put("bootstrap.servers", "szhua:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /*创建生产者*/
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hell1" + i, "world1" + i);
            /* 发送消息*/
            producer.send(record);
        }
        /*关闭生产者*/
        producer.close();
    }
}
