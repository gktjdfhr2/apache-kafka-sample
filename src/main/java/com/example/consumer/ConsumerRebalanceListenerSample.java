package com.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ConsumerRebalanceListenerSample implements ConsumerRebalanceListener{
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";
    private final static String TOPIC_NAME = "test";
    private static KafkaConsumer kafkaConsumer;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffset;

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // KafkaProducer에서 직렬화한 방식과 동일한 타입의 역직렬화를 사용해야 한다.
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //리밸런싱 발생 시 수동 커밋을 하기 위한 자동 커밋 미설정
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        kafkaConsumer = new KafkaConsumer(properties);
        // 리밸런싱을 감지하기 위핸 ConsumerRebalancelistener를 구독에 포함
        kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME), new ConsumerRebalanceListenerSample());

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            currentOffset = new HashMap<>();

            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
                currentOffset.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null)
                );
                kafkaConsumer.commitSync(currentOffset);
            }
        }
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.warn("onPartitionsRevoked: {}", partitions);
        // 리밸런싱 발생 시 가장 마지막에 처리한 오프셋을 커밋해줌
        kafkaConsumer.commitSync(currentOffset);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.warn("onPartitionsAssigned: {}", partitions);
    }
}
