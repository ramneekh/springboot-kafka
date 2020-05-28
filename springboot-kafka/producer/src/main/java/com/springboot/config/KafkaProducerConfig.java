package com.springboot.config;

import java.util.HashMap;
import java.util.Map;

import com.springboot.model.Job;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@EnableKafka
@Configuration
public class KafkaProducerConfig {

	@Value("${kafka.boot.server}")
	private String kafkaServer;
	@Value("${kafka.topic.requestreply-topic}")
	private String requestReplyTopic;
	@Value("${kafka.consumer.group.id}")
	private String kafkaGroupId;
	@Bean
	public KafkaTemplate<String, Job> kafkaTemplate() {
		return new KafkaTemplate<>(producerConfig());
	}

	@Bean
	public ProducerFactory<String, Job> producerConfig() {
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return new DefaultKafkaProducerFactory<>(config);
	}

//required for request reply
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
	public ReplyingKafkaTemplate<String, Job, Job> replyKafkaTemplate(ProducerFactory<String, Job> pf, KafkaMessageListenerContainer<String, Job> container){
		return new ReplyingKafkaTemplate<>(pf, container);
	}

	@Bean
	public KafkaMessageListenerContainer<String, Job> replyContainer(ConsumerFactory<String, Job> cf) {
		ContainerProperties containerProperties = new ContainerProperties(requestReplyTopic);
		return new KafkaMessageListenerContainer<>(cf, containerProperties);
	}


}
