package com.kafka.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FIleWriter {
	
	private static final Logger LOG = LoggerFactory.getLogger(FIleWriter.class);
	
	public static void writeRecordsToFile(Iterator<ConsumerRecord<String, String>> records) {
		String userDir = System.getProperty("user.dir"); 
        Path filePath = Paths.get(userDir, "src/main/resources/consumer.txt");
        try {
            if (!Files.exists(filePath)) {
                Files.createFile(filePath);
            }
            ConsumerRecord<String, String> aRecord= null;
            while (records.hasNext()) {
            	aRecord= records.next();
            	LOG.info("Key: {}, Value:{}",aRecord.key(),aRecord.value());
                Files.write(filePath, (aRecord.key()+" "+aRecord.value() 
                		+ System.lineSeparator()).getBytes(),StandardOpenOption.APPEND);
                aRecord=null;
            }
            LOG.info("Records written to file: " + filePath.toAbsolutePath());
        } catch (IOException e) {
        	LOG.error("Error writing to file: " + e.getMessage());
        }
    }	
}