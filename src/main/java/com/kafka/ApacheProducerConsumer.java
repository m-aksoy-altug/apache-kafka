package com.kafka;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.config.H2Connector;
import com.kafka.producer.ProducerFromH2;
import com.kafka.prop.SettingProperties;
import com.kafka.utils.Constants;
import com.kafka.utils.FIleWriter;
import com.kafka.utils.GenerateData;


public class ApacheProducerConsumer {
	private static final Logger LOG = LoggerFactory.getLogger(ApacheProducerConsumer.class);
	
	public static void main(String[] args) {
		// clean install compile exec:java -Dexec.mainClass="com.kafka.ApacheProducerConsumer"	
		 ExecutorService executorService = Executors.newFixedThreadPool(3);		 
		 Future<?> producerFuture = executorService.submit(ApacheProducerConsumer::producerStart);
		 Future<?> streamFuture =  executorService.submit(ApacheProducerConsumer::streamStart);
		 Future<?> consumerFuture = executorService.submit(ApacheProducerConsumer::consumerStart);
		 executorService.shutdown();
         try {
             executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
             if (producerFuture.isDone() && streamFuture.isDone() && consumerFuture.isDone()) {
            	 LOG.info("All initial tasks completed. Starting the next task...");
                 ExecutorService singleExec = Executors.newSingleThreadExecutor();
                 singleExec.submit(ProducerFromH2::producerStart);
                 singleExec.shutdown();
             }
         } catch (Exception e) {
             LOG.error("Main thread interrupted: " + e.getMessage());
         } 
	}
	
	private static void consumerStart() {
		LOG.info("Consumer started on thread: " + Thread.currentThread().getName());
		Properties propsCons= SettingProperties.setConsumer();
		try(KafkaConsumer<String, String> consumer=new KafkaConsumer<String,String>(propsCons)) {	
			consumer.subscribe(Arrays.asList(Constants.KAFKA_TOPIC_NEW));
			consumer.poll(Duration.ofMillis(1000));
			consumer.seekToBeginning(consumer.assignment());
			final int maxOfTry=5;
			int currentTry=0;
			while(true) {
				final ConsumerRecords<String, String> consRecords=
						consumer.poll(Duration.ofMillis(30_000));
				if(consRecords.count()==0) {
					currentTry++;
					if(currentTry>maxOfTry) {
						break;
					}else {
						continue;
					}
				}
				Iterator<ConsumerRecord<String, String>> iteratorRecord= 
						consRecords.iterator();
				FIleWriter.writeRecordsToFile(iteratorRecord);
				consumer.commitAsync();
			}
			consumer.close();
			LOG.info("Consuming records is completed...");
		}catch(Exception e) {
			LOG.error(e.getMessage());
		}
	}
	
	private static void streamStart() {
		LOG.info("Stream started on thread: " + Thread.currentThread().getName());
		Properties propsStream= SettingProperties.setStream();
			StreamsBuilder strmBuldr= new StreamsBuilder();
			KStream<String,String> streamData = strmBuldr.stream(Constants.KAFKA_TOPIC);
			streamData
	          .filter((k, v) -> k != null && k.trim().contains("Cust0"))
	          .to(Constants.KAFKA_TOPIC_NEW);
			Topology topology = strmBuldr.build();
		try(KafkaStreams stream=new KafkaStreams(topology, propsStream);) {	
			stream.start();
			Thread.sleep(5000);
		}catch(Exception e) {
			LOG.error(e.getMessage());
		}
	}
	
	private static void producerStart() {
		LOG.info("Producer started on thread: " + Thread.currentThread().getName());
		Properties props =SettingProperties.setProducer(Constants.KAFKA_CLIENT_ID);
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
						new ProducerRecord<String, String>(Constants.KAFKA_TOPIC, customerId,strbld.toString());
				Future<RecordMetadata> ftrMetaData= producer.send(producerRecord);
				LOG.info("RecordMetaData:"+ftrMetaData.isDone());
			} while (x < 1000);
			//producer.commitTransaction();
		}catch(Exception e) {
			LOG.error(e.getMessage());
		}
	}
	
	
	
}