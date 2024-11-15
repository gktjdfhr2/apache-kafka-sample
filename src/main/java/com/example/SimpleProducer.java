package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer {

    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageKey = "Pangyo";
        String messageValue = "testMessage";
        int partitionNo = 0;

        // 메시지 키를 비워놓은 상태로 전송
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        // 메시지 키를 담아 전송
        ProducerRecord<String, String> record2 = new ProducerRecord<>("test", "Pangyo", "23");
        // 파티션을 지정해 전송
        ProducerRecord<String, String> record3 = new ProducerRecord<>(TOPIC_NAME, partitionNo, messageKey, messageValue);

        producer.send(record);
        producer.send(record2);
        producer.send(record3);
        logger.info("{}", record);
        producer.flush();
        producer.close();


    }
}
