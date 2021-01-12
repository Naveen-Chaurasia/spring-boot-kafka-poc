package com.naveen.springkafka;

import java.util.Date;
import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class SpringKafkaApplication implements ApplicationRunner {
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void sendMessage(String msg) {
		System.out.println("msg:" + msg);
		kafkaTemplate.send("naveen", msg);
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaApplication.class, args);
	}

	@KafkaListener(topics = "naveen", groupId = "group-id")
	public void listen(String message) {
		System.out.println(message);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		while (true) {
			Thread.currentThread().sleep(1000);
			sendMessage("{\"timestamp\":\"" + new Date() + "\", temp=" + randomNumber() + "}");
		}
	}

	private double randomNumber() {
		Random rand = new Random();
		return rand.nextDouble();
	}
}
