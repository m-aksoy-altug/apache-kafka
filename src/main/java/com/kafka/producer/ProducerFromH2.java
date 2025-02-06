package com.kafka.producer;

import java.io.DataOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.config.H2Connector;
import com.kafka.prop.SettingProperties;
import com.kafka.utils.Constants;
import com.kafka.utils.GenerateData;

public class ProducerFromH2 {
	private static final Logger LOG = LoggerFactory.getLogger(ProducerFromH2.class);

	public static void producerStart()  {
		LOG.info("New Producer started on thread: " + Thread.currentThread().getName());
		H2Connector.connect();
		ResultSet resultSet = H2Connector.fetchData();
		Properties props =SettingProperties.setProducer(Constants.KAFKA_CLIENT_ID_H2);
		try (final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);){	
			LOG.info("Connection is done");
			while(resultSet.next()) { // Silent fails if no data presents
              String companyName = resultSet.getString("companyName");
              String productName = resultSet.getString("productName");
              LOG.info("Company: {} | Product: {}: " ,companyName, productName);
				final ProducerRecord<String,String> producerRecord= 
						new ProducerRecord<String, String>(Constants.KAFKA_TOPIC_H2, 
								companyName,productName);
				Future<RecordMetadata> ftrMetaData= producer.send(producerRecord);
				LOG.info("RecordMetaData:"+(ftrMetaData.isDone() ? 
						("Offset:"+ftrMetaData.get().offset()+" Partition: "+ftrMetaData.get().partition())
						:"Still in progress"));
			}
		}catch(Exception e) {
			LOG.error(e.getMessage());
		}finally {
			H2Connector.closeConnection();
		}
	}
	
	

	
}
