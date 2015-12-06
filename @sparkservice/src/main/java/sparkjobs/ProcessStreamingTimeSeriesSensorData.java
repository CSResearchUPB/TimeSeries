package sparkjobs;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.parser.JSONParser;

public class ProcessStreamingTimeSeriesSensorData {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	public static void main(String[] args) {

		JSONParser parser = new JSONParser();

		SparkConf conf = new SparkConf().setAppName("spark").setMaster("local");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

		System.out.println("Running spark");

		// Set<String> topics = new HashSet<>();
		// Map<String, String> kafkaParams = new HashMap<>();
		// topics.add("test");
		// kafkaParams.put("metadata.broker.list", "localhost:9092");
		//
		// JavaPairInputDStream<String, String> messages =
		// KafkaUtils.createDirectStream(jssc, String.class, String.class,
		// StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		//
		// messages.print();
		//
		// // Prepare the data
		//
		// messages.foreachRDD(new Function<JavaPairRDD<String, String>, Void>()
		// {
		//
		// @Override
		// public Void call(JavaPairRDD<String, String> v1) throws Exception {
		//
		// for (Tuple2<String, String> tuple : v1.collect()) {
		//
		// JSONObject json = (JSONObject) parser.parse(tuple._2);
		//
		// System.out.println(json);
		//
		// }
		// return null;
		//
		// }
		// });

		List<Vector> vectorList = new ArrayList() {
			{
				add(Vectors.dense(1.0, 0.0, 3.0));
				add(Vectors.dense(2.0, 0.0, 3.0));
				add(Vectors.dense(3.0, 0.0, 3.0));
				add(Vectors.dense(1.0, 5.0, 3.0));
			}
		};

		JavaRDD<Vector> vectorRDD = jssc.sparkContext().parallelize(vectorList);

		MultivariateStatisticalSummary summary = Statistics.colStats(vectorRDD.rdd());

		Vector v = summary.variance();

		System.out.println(summary.mean()); // a dense vector containing the
											// mean value for each column
		System.out.println(v); // column-wise variance
		System.out.println(summary.numNonzeros()); // number of nonzeros in each
													// column

		double[] t = v.toArray();

		for (int i = 0; i < t.length; i++) {

			System.out.println(Math.sqrt(t[i]));

		}

		// Math.sqrt();

		// Start the computation
		jssc.start();
		jssc.awaitTermination();

	}
}
