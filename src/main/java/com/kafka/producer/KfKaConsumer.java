package com.kafka.producer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KfKaConsumer {
	public void consumer(){
		Properties props = new Properties();
	    props.put("bootstrap.servers", "127.0.0.1:9092");
	    props.put("group.id", "test");
	    props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("session.timeout.ms", "30000");
	    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	    consumer.subscribe(Arrays.asList("my-top"));
	    while (true) {
	      ConsumerRecords<String, String> records = consumer.poll(100);
	      for (ConsumerRecord<String, String> record : records)
	        System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
	    }
	}
	
	public static void main(String[] args){
		new KfKaConsumer().consumer();
	}

}
