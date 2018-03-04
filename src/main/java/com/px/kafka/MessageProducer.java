package com.px.kafka;

import kafka.producer.KeyedMessage;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MessageProducer {

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;

    public MessageProducer(String topic, String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "tbds-172-16-10-44:6668");
        props.put("key.serializer", ByteArraySerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    public void produceMsg(){

        String[] words = new String[]{
                "or 420 million US dollars",
                "What happened is that a group",
                "fight lasted hours overnight between",
                "the air according to the residents",
                "told me that one Malian soldier",
                "military spokesman says security forces",
                "that thousands of people who prayed",
                "continuing to receive treatment for",
                "freezing temperatures currently gripping",
                "Central African Republic Michel Djotodia",
                "freezing temperatures currently gripping",
                "former opposition will make up most",
                "The Syrian government has accused",
                "Doctors in South Africa reporting",
                "military spokesman says security forces",
                "Late on Monday, Ms Yingluck invoked special powers allowing officials to impose curfews",
                "Those who took up exercise were three times more likely to remain healthy over the next eight",
                "The space dream, a source of national pride and inspiration",
                "There was no time to launch the lifeboats because the ferry capsized with such alarming speed"
        };
        Random random = new Random();
        // 构建发送的消息
        String word = words[random.nextInt(words.length)];
        System.out.println("发送消息: " + word);

        /**
         * topic: 消息的主题
         * key：消息的key，同时也会作为partition的key
         * message:发送的消息
         */
        ProducerRecord<byte[], byte[]> data = new ProducerRecord<byte[], byte[]>(topic, word.getBytes(),word.getBytes());
        Future<RecordMetadata> send = producer.send(data);
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("开始发送消息 ...");
        MessageProducer producer = new MessageProducer("stormtest", args);
        while(true){
            producer.produceMsg();
        }
    }
}
