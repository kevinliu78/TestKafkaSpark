package com.pbn.pnm;
/**
 * @author kevin
 * @version 创建时间: 2018年4月17日上午11:00:46
 * @ClassName 类名称
 * @Description 类描述
 */
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.StringDeserializer;
public class SprakStreamingKfkaTest {
	
	public static void main(String[] args) {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "192.168.16.179:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("portal1");

//		JavaInputDStream<ConsumerRecord<String, String>> stream =
//		  KafkaUtils.createDirectStream(
//		    streamingContext,
//		    LocationStrategies.PreferConsistent(),
//		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//		  );
//
//		stream.mapToPair(record -> 
//			new Tuple2<>(record.key(), record.value());
//			System.err.println("----========================");
//			System.err.println(record.value());
//		});
	}

}
