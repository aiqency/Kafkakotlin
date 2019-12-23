package ch.aiqency.kafkakotlin

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.kafka.core.KafkaAdmin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.annotation.KafkaListener

import org.springframework.kafka.core.ProducerFactory
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.util.*
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.annotation.EnableKafka




@SpringBootApplication
class KafkakotlinApplication

fun main(args: Array<String>) {
	runApplication<KafkakotlinApplication>(*args)
}

/**
* Rest that test the communication
*/
@RestController
class Ctrl(val kafkaProducer: MKafkaProducer){

	@RequestMapping(value = "/", method = [RequestMethod.GET])
	fun rest() = kafkaProducer.send( "Kafka message")

	@RequestMapping(value = "/test", method = [RequestMethod.GET])
	fun test() {
		var i = 0
		while (true)
			kafkaProducer.send( "Kafka message ${i++}")
	}
}

/**
* Producer related code
*/
@Service
class MKafkaProducer {

	@Value(value = "\${kafka.bootstrapAddress}")
	lateinit var bootstrapAddress: String

	val kafkaProducer: KafkaProducer<String, String> by lazy {
		val configProps = Properties()
		configProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapAddress
		configProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
		configProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
		KafkaProducer<String, String>(configProps)
	}

	fun send(msg: String) {
		kafkaProducer.send(ProducerRecord("myTopic", msg))
	}
}

/**
* Consumer related code
*/
@Service
class KMafkaConsumer {

	@KafkaListener(topics = ["myTopic"], groupId = "group_id_1")
	fun consume(message: String) {
		System.out.println("Consumed message: " + message);
	}
	
}

@EnableKafka
@Configuration
class KafkaConfiguration {

	@Value(value = "\${kafka.bootstrapAddress}")
	lateinit var bootstrapAddress: String

	@Bean
	fun consumerFactory(): ConsumerFactory<String, Any> {
		val config = HashMap<String, Any>()
		config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapAddress
		config[ConsumerConfig.GROUP_ID_CONFIG] = "group_id"
		config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
		config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
		return DefaultKafkaConsumerFactory(config)
	}

	@Bean
	fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
		val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
		factory.consumerFactory = consumerFactory()
		return factory
	}

}

