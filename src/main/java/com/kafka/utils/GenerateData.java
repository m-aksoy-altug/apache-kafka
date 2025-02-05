package com.kafka.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class GenerateData {
	
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
