package com.springboot.ctrl;

import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.springboot.model.Job;
import com.springboot.service.KafkaSender;

@RestController
public class KafkaProducerController {

	@Autowired
	private KafkaSender sender;

	@PostMapping
	@RequestMapping("/kafkaProducer")
	public ResponseEntity<String> sendRequest(@RequestBody Job job){
		sender.sendData(job);
		return new ResponseEntity<>("Data sent to Kafka", HttpStatus.OK);
	}

	@SneakyThrows
	@PostMapping
	@RequestMapping("/kafkaProducerReply")
	public Job sendRequestReply(@RequestBody Job job){
		return sender.sendRequstReplyMessage(job);
		//return new ResponseEntity<>("Data sent to Kafka", HttpStatus.OK);
	}
}
