package com.mastek.mapr.stream;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.JSONException;
import twitter4j.JSONObject;

public class MapRStreamProducer {

	// Declare a new producer
    public static KafkaProducer producer;
    public String topic = "/twitter:tweets";
    
	public MapRStreamProducer() {
		Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        
        producer = new KafkaProducer<String, String>(props);
	}
	
	public void produce(String userName, String line){
		JSONObject record = new JSONObject();
		try {
			record.put("user_name", userName);
			record.put("tweet", line);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, record.toString());
        producer.send(rec);
        
	}
	
	public void stopProducer(){
		producer.close();
	}
}
