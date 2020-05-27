package com.springboot.service;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.springboot.model.Job;

@Service
public class KafkaSender {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSender.class);

	@Autowired
	private KafkaTemplate<String, Job> kafkaTemplate;

	@Value("${kafka.topic.name}")
	private String topicName;


	public void sendMessage(Job job) {
		// TODO Auto-generated method stub
		Map<String, Object> headers = new HashMap<>();
		headers.put(KafkaHeaders.TOPIC, topicName);
		kafkaTemplate.send(new GenericMessage<Job>(job, headers));
		// use the below to send String values through kafka
		// kafkaTemplate.send(topicName, "some string value")
		LOGGER.info("Data - " + job.toString() + " sent to Kafka Topic - " + topicName);
	}

	public void sendData(Job job) {
        
		Map<String, Object> headers = new HashMap<>();
		headers.put(KafkaHeaders.TOPIC, topicName);

		ListenableFuture<SendResult<String, Job>> future =
	      kafkaTemplate.send(new GenericMessage<Job>(job, headers));
	     
	    future.addCallback(new ListenableFutureCallback<SendResult<String, Job>>() {
	 
	        @Override
	        public void onSuccess(SendResult<String, Job> result) {
	        	LOGGER.info("Sent message=[{}] with offset=[{}]",job.getJobId(),result.getRecordMetadata().offset());
	        }
	        @Override
	        public void onFailure(Throwable ex) {
	        	LOGGER.error("Unable to send message=[{}] due to : {}",job.getJobId(), ex.getMessage());
	        }
	    });
	}	
	
}
