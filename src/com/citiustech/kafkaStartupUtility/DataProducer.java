package com.citiustech.kafkaStartupUtility;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DataProducer {
	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);

		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);
		System.out.println("Enter the data to publish");
		String data = sc.nextLine();

		if (!data.equalsIgnoreCase("")) {
			ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("MyFirstTopic", 1,
					data);
			producer.send(producerRecord);
			System.out.println("Producer has sent data successfully");
		}
		producer.close();
		sc.close();
	}
}

//https://www.youtube.com/watch?v=jzUJPJvWQ18
