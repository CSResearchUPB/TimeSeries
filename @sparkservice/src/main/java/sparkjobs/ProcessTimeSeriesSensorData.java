package sparkjobs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class ProcessTimeSeriesSensorData {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("spark").setMaster("local");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

		System.out.println("Running spark");

		Set<String> topics = new HashSet<>();
		Map<String, String> kafkaParams = new HashMap<>();
		topics.add("test");
		kafkaParams.put("metadata.broker.list", "localhost:9092");

		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		messages.print();

		// Start the computation
		jssc.start();
		jssc.awaitTermination();

	}
}
