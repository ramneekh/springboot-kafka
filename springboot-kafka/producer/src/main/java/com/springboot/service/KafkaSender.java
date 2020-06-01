package com.springboot.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
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

	@Autowired
	ReplyingKafkaTemplate<String, Job,Job> kafkaReplyTemplate;

	@Value("${kafka.topic.name}")
	private String topicName;

	@Value("${kafka.topic.requestreply-topic}")
	String requestReplyTopic;


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


	public Job sendRequestReplyMessage(Job job) throws ExecutionException, InterruptedException {

		LOGGER.info("Data - " + job.toString() + " sent to Kafka Topic - " + topicName);
		// create producer record
		ProducerRecord<String, Job> record = new ProducerRecord<String, Job>(topicName, job);
		// set reply topic in header
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
		// post in kafka topic
		RequestReplyFuture<String, Job, Job> sendAndReceive = kafkaReplyTemplate.sendAndReceive(record);

		// confirm if producer produced successfully
		SendResult<String, Job> sendResult = sendAndReceive.getSendFuture().get();

		//print all headers
		sendResult.getProducerRecord().headers().forEach(header -> System.out.println(header.key() + ":" + header.value().toString()));

		// get consumer record
		ConsumerRecord<String, Job> consumerRecord = sendAndReceive.get();
		// return consumer value
		return consumerRecord.value();

	}


	
}
