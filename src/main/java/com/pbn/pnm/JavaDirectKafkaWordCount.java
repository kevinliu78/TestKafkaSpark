package com.pbn.pnm;

import java.util.ArrayList;
import java.util.Arrays;
/**
 * @author kevin
 * @version 创建时间: 2018年4月17日上午11:26:55
 * @ClassName 类名称
 * @Description 类描述
 */
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 *
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ topic1,topic2
 */

public final class JavaDirectKafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static KafkaProducer<Integer, String> producer;

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
					+ "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}
		//init producer
		initProducer();
		
		// StreamingExamples.setStreamingLogLevels();

		String brokers = args[0];
		String topics = args[1];

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(1000));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("group.id", "group1");
		kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		// Create direct kafka stream with brokers and topics
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

		JavaDStream<String> lines = messages
				.mapPartitions(new FlatMapFunction<Iterator<ConsumerRecord<String, String>>, String>() {
					@Override
					public Iterator<String> call(Iterator<ConsumerRecord<String, String>> t) throws Exception {
						List<String> list = new ArrayList<>();
						t.forEachRemaining(record -> {
							System.out.println("=======1111==========" + record.value());
							String message = record.value();
							sendMessage(message);
							list.add(record.value());
						});
						return list.iterator();
					}
				}).filter((String s) -> {
					return (!s.contains("abcdef"));
				});

		JavaDStream<String> ssssss = lines.map((String s) -> {
			System.out.println("=======2222==========" + s);
			return s;
		});

		ssssss.print();
		// Get the lines, split them into words, count the words and print
		// JavaDStream<String> lines = messages.map(ConsumerRecord::value);
		// lines.print();

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	protected static void initProducer() {
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.16.179:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<Integer, String>(props);
	}

	// 发送消息
	protected static void sendMessage(String message) {
		if(producer == null) {
			initProducer();
		}
		producer.send(new ProducerRecord<Integer, String>("spark", message));

	}

	class DemoCallBack implements Callback {

		private final String message;

		public DemoCallBack(String message) {
			this.message = message;
		}

		/**
		 * A callback method the user can implement to provide asynchronous handling of
		 * request completion. This method will be called when the record sent to the
		 * server has been acknowledged. Exactly one of the arguments will be non-null.
		 *
		 * @param metadata
		 *            The metadata for the record that was sent (i.e. the partition and
		 *            offset). Null if an error occurred.
		 * @param exception
		 *            The exception thrown during processing of this record. Null if no
		 *            error occurred.
		 */
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			if (metadata != null) {
			} else {
				exception.printStackTrace();
			}
		}
	}
}
