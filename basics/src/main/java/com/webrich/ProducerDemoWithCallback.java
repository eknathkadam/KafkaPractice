package com.webrich;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello world!");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++){
            String topic =  "ORDER_PROCESSING_COMPLETED";
            String value = "Hi from Java "+i;
            String key = "id_"+i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,key,value );
            producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                log.info("Topic: " + metadata.topic() + "\n" +
                                        "Key: " + producerRecord.key() + "\n" +
                                        "Partition: " + metadata.partition() + "\n" +
                                        "Offset: " + metadata.offset() + "\n" +
                                        "Timestamp:" + metadata.timestamp());
                            } else {
                                log.error("Error while producing", exception);
                            }
                        }
                    }
            );
        }

        producer.flush();
        producer.close();

    }


}