package com.kafka.prop;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import com.kafka.utils.Constants;

public class SettingProperties {
	
	public static Properties setProducer(String kafkaTopic) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaTopic);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // default 16Mb
		//setting to ensure data is replicated to all replicas before acknowledging the write.
		props.put(ProducerConfig.ACKS_CONFIG, "all"); // default all
		props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000"); // default 120_000. 2mins
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000"); // default 30000, 30s
		//props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");  // Transactional producer
		return props;
	}
	
	
	public static Properties setStream() {
		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG,Constants.KAFKA_STREAM_ID);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
		return props;
	}
	
	public static Properties setConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
		props.put(ConsumerConfig.GROUP_ID_CONFIG,Constants.KAFKA_CONSUMER_GROUP_ID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		// Fine tune is important
		props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,104_857_600); // 100MB// default52428800 (50 MB)
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,50_000); // 50KB, default 1 byte  
		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,1_048_576); // default 1MB
		return props;
	}
	
}
