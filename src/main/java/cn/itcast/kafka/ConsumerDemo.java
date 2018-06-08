package cn.itcast.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author lishengping
 * @date 2018/06/01
 */
public class ConsumerDemo {
    @Test
    public void testConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop03:9092,hadoop04:9092,hadoop05:9092");
        props.put("group.id", "lsp-test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("MQ_TOPIC_KAFKA_CLICK_DATA"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset());
            }
            consumer.commitSync();
        }
    }
}
