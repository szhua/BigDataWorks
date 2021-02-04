package com.szhua.kafkawork.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerClient  {


    public static void main(String[] args) throws  Exception{
        String topic = "flume-kafka";
        String group = "group1";
        Properties props = new Properties();
        props.put("bootstrap.servers", "szhua:9092");
        props.put("group.id", group);
        props.put("enable.auto.commit", false);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> partitions = new ArrayList<>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

        /*可以指定读取哪些分区 如这里假设只读取主题的0分区*/
        for (PartitionInfo partition : partitionInfos) {
            if (partition.partition()==0){
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
            }
        }
        // 为消费者指定分区
        consumer.assign(partitions);
        do {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            for (ConsumerRecord<Integer, String> record : records) {
                System.err.printf("partition = %s, key = %s, value = %s\n", record.partition(), record.key(), record.value());
            }
            consumer.commitSync();
        } while (true);
    }



}
