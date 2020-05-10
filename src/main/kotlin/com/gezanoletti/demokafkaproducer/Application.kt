package com.gezanoletti.demokafkaproducer

import com.gezanoletti.demokafkaproducer.KafkaConstants.TOPIC_NAME_AVRO
import com.gezanoletti.demokafkaproducer.KafkaConstants.TOPIC_NAME_COMPACT
import com.gezanoletti.demokafkaproducer.KafkaConstants.TOPIC_NAME_NORMAL
import com.gezanoletti.demokafkaproducer.avro.PersonMessageKey
import com.gezanoletti.demokafkaproducer.avro.PersonMessageValue
import com.github.javafaker.service.FakeValuesService
import com.github.javafaker.service.RandomService
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.lang.Thread.sleep
import java.util.*
import java.util.concurrent.ExecutionException


fun main() {
    val producer1: Producer<String, String> = KafkaProducer(
        mapOf(
            BOOTSTRAP_SERVERS_CONFIG to KafkaConstants.KAFKA_BROKERS,
            CLIENT_ID_CONFIG to KafkaConstants.CLIENT_ID,
            KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name
        )
    )

    val producer2: Producer<PersonMessageKey, PersonMessageValue> = KafkaProducer(
        mapOf(
            BOOTSTRAP_SERVERS_CONFIG to KafkaConstants.KAFKA_BROKERS,
            CLIENT_ID_CONFIG to KafkaConstants.CLIENT_ID,
            KEY_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java.name,
            VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java.name,
            "schema.registry.url" to "http://localhost:8081"
        )
    )

    val fakeValuesService = FakeValuesService(Locale.ENGLISH, RandomService())
    for (i in 0..10) {
        val message = fakeValuesService.bothify("????##@gmail.com")
        val producerRecordNormal = ProducerRecord(TOPIC_NAME_NORMAL, i.toString(), message)
        val producerRecordCompact = ProducerRecord(TOPIC_NAME_COMPACT, i.toString(), message)

        val producerRecordAvro = ProducerRecord(
            TOPIC_NAME_AVRO,
            PersonMessageKey(i),
            PersonMessageValue(
                i,
                fakeValuesService.letterify("????????"),
                fakeValuesService.letterify("????????")
            )
        )

        try {
            val metadataNormal = producer1.send(producerRecordNormal).get()
            println("Record sent to topic ${metadataNormal.topic()}, to partition ${metadataNormal.partition()}, with offset ${metadataNormal.offset()}")

            val metadataCompact = producer1.send(producerRecordCompact).get()
            println("Record sent to topic ${metadataCompact.topic()}, to partition ${metadataCompact.partition()}, with offset ${metadataCompact.offset()}")

            val metadataAvro = producer2.send(producerRecordAvro).get()
            println("Record sent to topic ${metadataAvro.topic()}, to partition ${metadataAvro.partition()}, with offset ${metadataAvro.offset()}")

        } catch (e: ExecutionException) {
            println("Error in sending record")
            println(e)

        } catch (e: InterruptedException) {
            println("Error in sending record")
            println(e)
        }

        sleep(500)
    }
}

object KafkaConstants {
    const val KAFKA_BROKERS = "localhost:9092"
    const val CLIENT_ID = "client2"
    const val TOPIC_NAME_NORMAL = "demo-basic-kafka-partitions"
    const val TOPIC_NAME_COMPACT = "demo-basic-kafka-partitions-compact"
    const val TOPIC_NAME_AVRO = "demo-basic-kafka-partitions-avro"
}
