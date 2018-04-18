package com.pbn.pnm;

import org.apache.spark.SparkConf;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public final class JavaKafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	private JavaKafkaWordCount() {
	}

	public static void main(String[] args) {
		try {
			if (args.length < 4) {
				System.err.println("Usage: JavaKafaWordCount <zkQuorum> <group> <topics> <numThreads>");
				System.exit(1);
			}

			StreamingExamples.setStreamingLogLevels();

			SparkConf sparkConf = new SparkConf().setAppName("java_kafka_word_count");
			// create the context with 2 seconds batch size
			JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

			int numThreads = Integer.parseInt(args[3]);
			Map<String, Integer> topicMap = new HashMap<>();
			String[] topics = args[2].split(",");
			for (String topic : topics) {
				topicMap.put(topic, numThreads);
			}

			JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1],
					topicMap);

			JavaDStream<String> lines = messages.map(Tuple2::_2);

			JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

			JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
					.reduceByKey((i1, i2) -> i1 + i2);
			System.err.println(
					"=-==============================================================================================");
			wordCounts.print();
			System.err.println(
					"=-==============================================================================================");
			jssc.start();
			jssc.awaitTermination();
		} catch (Exception e) {

		}
	}

}