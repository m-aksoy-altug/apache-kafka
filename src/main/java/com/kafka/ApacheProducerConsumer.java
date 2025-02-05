package com.kafka;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ApacheProducerConsumer {
	private static final Logger LOG = LoggerFactory.getLogger(ApacheProducerConsumer.class);
	private static final String KAFKA_SERVER = "192.168.1.113:9094";
	private static final String KAFKA_TOPIC = "stream-test";
	private static final String KAFKA_STREAM_ID = "stream-analysis-test";
	private static final String KAFKA_TOPIC_NEW = "stream-test-filtered";

	public static void main(String[] args) {
		// clean install compile exec:java -Dexec.mainClass="com.kafka.ApacheProducerConsumer"
		 ExecutorService executorService = Executors.newFixedThreadPool(2);		 
         executorService.submit(ApacheProducerConsumer::producerStart);
         executorService.submit(ApacheProducerConsumer::streamStart);
         executorService.shutdown();
	}
	
	private static void streamStart() {
		LOG.info("Stream started on thread: " + Thread.currentThread().getName());
		Properties propsStream= setStream();
			StreamsBuilder strmBuldr= new StreamsBuilder();
			KStream<String,String> streamData = strmBuldr.stream(KAFKA_TOPIC);
			streamData
	          .filter((k, v) -> k != null && k.trim().contains("Cust0"))
	          .to(KAFKA_TOPIC_NEW);
			Topology topology = strmBuldr.build();
		try(KafkaStreams stream=new KafkaStreams(topology, propsStream);) {	
			stream.start();
			Thread.sleep(5000);
		}catch(Exception e) {
			LOG.error(e.getMessage());
		}
	}
	
	private static Properties setStream() {
		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG,KAFKA_STREAM_ID);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
		return props;
	}
	
	private static void producerStart() {
		LOG.info("Producer started on thread: " + Thread.currentThread().getName());
		Properties props =setProducer();
		try (final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);){	
			// producer.initTransactions(); // Transactional producer
			int x = 0;
			do {
				// producer.beginTransaction();
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
				Future<RecordMetadata> ftrMetaData= producer.send(producerRecord);
				LOG.info("RecordMetaData:"+ftrMetaData.isDone());
			} while (x < 1000);
			//producer.commitTransaction();
		}catch(Exception e) {
			LOG.error(e.getMessage());
		}
	}
	
	
	private static Properties setProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, KAFKA_TOPIC);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // default 16Mb
		props.put(ProducerConfig.ACKS_CONFIG, "all"); // default all
		props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000"); // default 120_000. 2mins
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000"); // default 30000, 30s
		//props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");  // Transactional producer
	return props;
	}
}

class GenerateData {
	private final static Random random = new Random();

	public static String generateCustomerId() {
		return String.format("Cust%03d", random.nextInt(120) + 1);
	}

	public static String generateTimestamp() {
		LocalDateTime startDate = LocalDateTime.of(2025, 2, 5, 14, 0, 0);
		long randomSeconds = random.nextInt(365 * 24 * 60 * 60); 
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
