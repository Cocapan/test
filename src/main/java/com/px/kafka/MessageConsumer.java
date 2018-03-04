package com.px.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.*;

public class MessageConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        /* 定义kakfa 服务的地址，不需要将所有broker指定上 */
        props.put("bootstrap.servers", "tbds-172-16-10-44:6668");
        /* 制定consumer group */
        props.put("group.id", "test");
        /* 是否自动确认offset */
        props.put("enable.auto.commit", "true");
        /* 自动确认offset的时间间隔 */
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        /* key的序列化类 */
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        /* value的序列化类 */
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
         /* 定义consumer */
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        /* 消费者订阅的topic, 可同时订阅多个 */
        consumer.subscribe(Arrays.asList("panx_test"));

        /* 读取数据，读取超时时间为100ms */
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
            for (ConsumerRecord<byte[], byte[]> record : records)
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), new String(record.key()), new String(record.value()));
        }
//        Properties props = new Properties();
//        props.put("zookeeper.connect", "tbds-172-16-10-44:2181,tbds-172-16-10-43:2181,tbds-172-16-10-45:2181");
//        props.put("group.id", "panxuan_test");
//        //zk连接超时
//        props.put("zookeeper.session.timeout.ms", "5000");
//        props.put("zookeeper.sync.time.ms", "2000");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("auto.offset.reset", "smallest");
//        props.put("security.protocol", "SASL_PLAINTEXT");
//        props.put("sasl.mechanism", "PLAIN");
//        ConsumerConfig config = new ConsumerConfig(props);
//        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
//        Map<String, Integer> topicCountMap = new HashMap<>();
//        topicCountMap.put("elvan_test", new Integer(1));
//        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
//        System.out.println(consumerMap.get("elvan_test").size());
//        KafkaStream<byte[], byte[]> stream = consumerMap.get("elvan_test").get(0);
//        ConsumerIterator<byte[], byte[]> it = stream.iterator();
//        while (it.hasNext()){
//            String str = new String(it.next().message());
//            System.out.println(str);
//        }
    }


}
