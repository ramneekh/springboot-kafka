package com.springboot.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.springboot.model.Job;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
public class KafkaConfig {

	@Value("${kafka.boot.server}")
	private String kafkaServer;

	@Value("${kafka.consumer.group.id}")
	private String kafkaGroupId;

	@Value("${kafka.topic.requestreply-topic}")
	private String requestReplyTopic;

	@Bean
	public KafkaTemplate<String, Job> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

/*
	@Bean
	public ReplyingKafkaTemplate<String, Job, Job> replyKafkaTemplate(ProducerFactory<String, Job> pf, KafkaMessageListenerContainer<String, Job> container){
		return new ReplyingKafkaTemplate<>(pf, container);
	}
*/
/*

	@Bean
	public KafkaMessageListenerContainer<String, Job> replyContainer(ConsumerFactory<String, Job> cf) {
		ContainerProperties containerProperties = new ContainerProperties(requestReplyTopic);
		return new KafkaMessageListenerContainer<>(cf, containerProperties);
	}
*/


	@Bean
	public ProducerFactory<String, Job> producerFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return new DefaultKafkaProducerFactory<>(config);
	}

	@Bean
	public ConsumerFactory<String, Job> consumerFactory() {
		// TODO Auto-generated method stub
		Map<String, Object> config = new HashMap<>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		return new DefaultKafkaConsumerFactory<>(config, null, new JsonDeserializer<Job>(Job.class));
	}

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Job>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Job> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setReplyTemplate(kafkaTemplate());  //required to reply
		factory.setErrorHandler(new SeekToCurrentErrorHandler(    //required to send messages to dead letter queue
				new DeadLetterPublishingRecoverer(kafkaTemplate()), new FixedBackOff(2000,2)));

		return factory;
	}

/*
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Job>> kafkaListenerContainerFactory(KafkaTemplate<String, Job> template) {
		ConcurrentKafkaListenerContainerFactory<String, Job> listener = new ConcurrentKafkaListenerContainerFactory<>();
		listener.setConsumerFactory(consumerConfig());
		listener.setErrorHandler(new SeekToCurrentErrorHandler(
				new DeadLetterPublishingRecoverer(template), new FixedBackOff(2000,2)));

		return listener;
	}
*/
/*
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Job> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory,
			KafkaTemplate<String, Job> template) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setConsumerFactory(consumerConfig());
		factory.setErrorHandler(new SeekToCurrentErrorHandler(
				new DeadLetterPublishingRecoverer(template), new FixedBackOff(2000,3)));

		return factory;
	}
*/


}
