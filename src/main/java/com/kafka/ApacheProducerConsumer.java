package com.kafka;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ApacheProducerConsumer {

	private static final Logger LOG = LoggerFactory.getLogger(ApacheProducerConsumer.class);


	private static final String KAFKA_SERVER = "192.168.1.113:9094";
	private static final String KAFKA_TOPIC = "stream-test";

	public static void main(String[] args) {
		// clean install compile exec:java -Dexec.mainClass="com.kafka.ApacheProducerConsumer"
		try (final KafkaProducer<String, String> producer = setProducer();){
			int x = 0;
			do {
				StringBuilder strbld= new StringBuilder();
				String customerId =GenerateData.generateCustomerId();
				strbld.append(customerId).append(" ")
					.append(GenerateData.generateTimestamp()).append(" ")
					.append(GenerateData.generateProductId()).append(" ")
					.append(GenerateData.generateIpAddress()).append(" ")
					.append(GenerateData.generateQuantity()).append(" ")
					.append(GenerateData.generateUrl());
				LOG.info("CustId: "+customerId);
				x++;
				final ProducerRecord<String,String> producerRecord= 
						new ProducerRecord<String, String>(KAFKA_TOPIC, customerId,strbld.toString());
						producer.send(producerRecord);	
			} while (x < 1000);
		}catch(Exception e) {
			LOG.error(e.getMessage());
		}
		
	}

	private static KafkaProducer<String, String> setProducer() {
		Properties props = new Properties();
		// host and port of brokers in the Kafka cluster
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
		// Setting kafka topic 
		props.put(ProducerConfig.CLIENT_ID_CONFIG, KAFKA_TOPIC);
		// Setting class for key part of the message
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// Setting class for value part of the message
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//		// Setting size of the batch of messages published in one produces
//		props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // default 16Mb
		// Ensuring full commit of all records in one produce
		props.put(ProducerConfig.ACKS_CONFIG, "all"); //all
		props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000"); // default 120_000. 2mins
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000"); // default 30000, 30s
		props.put("log-level", "DEBUG"); 
	return new KafkaProducer<String, String>(props);
	}
}

class GenerateData {
	private final static Random random = new Random();

	public static String generateCustomerId() {
		return String.format("Cust%03d", random.nextInt(120) + 1);
	}

	public static String generateTimestamp() {
		LocalDateTime startDate = LocalDateTime.of(2025, 2, 5, 14, 0, 0);
		long randomSeconds = random.nextInt(365 * 24 * 60 * 60); // Random seconds within a year
		LocalDateTime randomDate = startDate.plusSeconds(randomSeconds);
		return randomDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss:000'Z'"));
	}

	public static String generateProductId() {
		return String.format("PROD%03d", random.nextInt(101) + 1);
	}

	public static String generateIpAddress() {
		return String.format("192.18.12.%d", random.nextInt(111) + 1);
	}

	public static String generateQuantity() {
		int[] quantities = { 10, 15, 30, 50, 75, 100, 111, 120, 150 };
		return String.valueOf(quantities[random.nextInt(quantities.length)]);
	}

	public static String generateUrl() {
		String[] urls = { "http://dummy.com/Acc11", "http://dummy.com/Mob11" };
		return urls[random.nextInt(urls.length)];
	}
}
