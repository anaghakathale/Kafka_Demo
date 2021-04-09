package com.citiustech.kafkaStartupUtility;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
public class DataConsumer {
	public static void main(String[] args) {
	try {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("group.id", "test-group");
		properties.put("enable.auto.commit", "true");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList("MyFirstTopic"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
			System.out.printf("offset = %d, key = %s, value= %s\n", record.offset(), record.key(),
						record.value());
		}
		consumer.close();
		}
	} catch (Exception e) {e.printStackTrace();}
}
}

// https://www.youtube.com/watch?v=wpCeOWS2IJo
