package sparkstreamer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class SparkHelloWorld {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("spark").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		System.out.println("Running spark");

		Set<String> topics = new HashSet<>();
		Map<String, String> kafkaParams = new HashMap<>();
		topics.add("test");
		kafkaParams.put("metadata.broker.list", "192.168.0.114:9092");

		JavaPairReceiverInputDStream<String, String> directKafkaStream = KafkaUtils
				.createDirectStream(sc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topics);

	}
}
