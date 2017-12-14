package com.whl.test;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerTest {

    public void custumerRev() {
        Properties props = new Properties();
        //服务器地址
        props.put("bootstrap.servers", "192.168.0.48:9092");
        //组编号；默认值为空
        props.put("group.id", "test18");
        //默认为true; true表示同步到zookeeper
        props.put("enable.auto.commit", "true");
        //consumer向zookeeper提交offset的频率
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test18")); //标题

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                //输出生产者生产的信息
                System.out.printf("=========offset = %d, key = %s, value = %s",
                        record.offset(), record.key(), record.value());
                System.out.println("");
            }
            //System.out.println("===========done===============");
        }
    }


    public static void main(String[] args) {
        KafkaConsumerTest test = new KafkaConsumerTest();
        test.custumerRev();
    }
}
