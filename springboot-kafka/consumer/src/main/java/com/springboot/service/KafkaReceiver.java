package com.springboot.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.springboot.model.Job;

@Service
public class KafkaReceiver {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiver.class);

/*	@KafkaListener(topics = "${kafka.topic.name}", groupId = "${kafka.consumer.group.id}")
	public void recieveData(@Payload Job job,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition
    		//,@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key
			) {
		LOGGER.info("Data:{} recieved on partition:{}",job.getJobId(),partition);
	}
	
	
	@KafkaListener(
			  topics = "${kafka.topic.name}", groupId = "${kafka.consumer.group.id}",
			  topicPartitions = @TopicPartition(topic = "${kafka.topic.name}",
			  partitionOffsets = {
			    @PartitionOffset(partition = "0", initialOffset = "0")
			    //,@PartitionOffset(partition = "3", initialOffset = "0")
			}))	
	public void receiveDataReplay(@Payload Job job,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition
    		//,@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key
			) {
		LOGGER.info("Data:{} replayed on partition:{}",job.getJobId(),partition);
	}
*/
	@KafkaListener(id = "fooGroup", topics = "${kafka.topic.name}")
	public void listen(Job in) {
		LOGGER.info("Received: " + in.getJobId());
		if (in.getFirstName().startsWith("foo")) {
			throw new RuntimeException("failed");
		}
	}

	@KafkaListener(id = "dltGroup", topics = "${kafka.topic.name}.DLT")
	public void dltListen(Job in) {
		LOGGER.info("Received from DLT: " + in.getJobId());
	}



}	
