package com.gezanoletti.demokafkaproducer

import com.gezanoletti.demokafkaproducer.KafkaConstants.TOPIC_NAME_COMPACT
import com.gezanoletti.demokafkaproducer.KafkaConstants.TOPIC_NAME_NORMAL
import com.github.javafaker.service.FakeValuesService
import com.github.javafaker.service.RandomService
import org.apache.commons.lang3.RandomUtils.nextInt
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.lang.Thread.sleep
import java.util.*
import java.util.concurrent.ExecutionException


fun main() {
    val props = mapOf(
        BOOTSTRAP_SERVERS_CONFIG to KafkaConstants.KAFKA_BROKERS,
        CLIENT_ID_CONFIG to KafkaConstants.CLIENT_ID,
        KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
        VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name
    )

    val producer: Producer<String, String> = KafkaProducer(props)

    val fakeValuesService = FakeValuesService(Locale.ENGLISH, RandomService())
    while (true) {
        val id = nextInt(1, 101).toString()
        val message = fakeValuesService.bothify("????##@gmail.com")
        val producerRecordNormal = ProducerRecord(TOPIC_NAME_NORMAL, id, message)
        val producerRecordCompact = ProducerRecord(TOPIC_NAME_COMPACT, id, message)

        try {
            val metadataNormal = producer.send(producerRecordNormal).get()
            println("Record sent to topic ${metadataNormal.topic()}, to partition ${metadataNormal.partition()}, with offset ${metadataNormal.offset()}")

            val metadataCompact = producer.send(producerRecordCompact).get()
            println("Record sent to topic ${metadataCompact.topic()}, to partition ${metadataCompact.partition()}, with offset ${metadataCompact.offset()}")

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
}
