package sparkjobs;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealVector;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class ProcessStreamingTimeSeriesSensorData {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	@SuppressWarnings("serial")
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

		JavaDStream<Tuple2<String, Vector>> vectorStream = messages
				.map(new Function<Tuple2<String, String>, Tuple2<String, Vector>>() {
					@Override
					public Tuple2<String, Vector> call(Tuple2<String, String> tuple2) {

						JsonNode json = null;
						try {
							json = objectMapper.readTree(tuple2._2);
						} catch (JsonProcessingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						String timestamp = json.path("record").path("sdata").path(0).path("timestamp").asText();
						double extTemp = json.path("record").path("sdata").path(0).path("sensors").path(0).path("value")
								.getDoubleValue();
						double intTemp = json.path("record").path("sdata").path(0).path("sensors").path(3).path("value")
								.getDoubleValue();
						double intUmd = json.path("record").path("sdata").path(0).path("sensors").path(4).path("value")
								.getDoubleValue();

						Vector values = Vectors.dense(extTemp, intTemp, intUmd);

						System.out.println(timestamp + " " + extTemp + " " + intTemp + " " + intUmd);

						Tuple2<String, Vector> tuple = new Tuple2<String, Vector>(timestamp, values);

						return tuple;
					}
				});

		JavaDStream<Tuple2<String, Vector>> windowVectorStream = vectorStream.window(Durations.seconds(10),
				Durations.seconds(10));

		windowVectorStream.foreachRDD(new Function<JavaRDD<Tuple2<String, Vector>>, Void>() {

			@Override
			public Void call(JavaRDD<Tuple2<String, Vector>> v1) throws Exception {

				System.out.println(v1.count());

				// Get vector data
				JavaRDD<Vector> vectorData = v1.map(tuple -> tuple._2);

				// Calc stats

				if (!vectorData.isEmpty()) {

					MultivariateStatisticalSummary summary = Statistics.colStats(vectorData.rdd());

					double[] m = summary.mean().toArray();
					double[] v = summary.variance().toArray();
					double[] std = new double[v.length];

					for (int i = 0; i < v.length; i++) {
						std[i] = Math.sqrt(v[i]);
					}

					RealVector mean = MatrixUtils.createRealVector(m);
					RealVector standardDeviation = MatrixUtils.createRealVector(std);
					double stdFactor = 1.5;

					RealVector minValues = mean.subtract(standardDeviation.mapMultiply(stdFactor));
					RealVector maxValues = mean.add(standardDeviation.mapMultiply(stdFactor));

					System.out.println(Vectors.dense(m)); // a dense vector
															// containing the
															// mean
					// value for each column
					System.out.println(Vectors.dense(v)); // column-wise
															// variance
					System.out.println(Vectors.dense(std)); // column-wise
															// standard
															// deviation

					JavaRDD<Tuple2<String, Vector>> outliers = v1
							.map(new Function<Tuple2<String, Vector>, Tuple2<String, Vector>>() {

						@Override
						public Tuple2<String, Vector> call(Tuple2<String, Vector> v1) throws Exception {

							RealVector values = MatrixUtils.createRealVector(v1._2.toArray());

							System.out.println("Values");
							for (int i = 0; i < values.getDimension(); i++) {
								System.out.println(values.getEntry(i));
							}

							System.out.println("-----------");

							System.out.println("MInValues");
							for (int i = 0; i < minValues.getDimension(); i++) {
								System.out.println(minValues.getEntry(i));
							}

							System.out.println("-----------");

							System.out.println("MAXValues");
							for (int i = 0; i < maxValues.getDimension(); i++) {
								System.out.println(maxValues.getEntry(i));
							}

							System.out.println("-----------");

							RealVector minOutliers = values.subtract(minValues);
							System.out.println("MinOutliers");
							for (int i = 0; i < minOutliers.getDimension(); i++) {
								System.out.println(minOutliers.getEntry(i));
							}

							System.out.println("-----------");

							RealVector maxOutliers = values.subtract(maxValues);
							System.out.println("MaxOutliers");
							for (int i = 0; i < maxOutliers.getDimension(); i++) {
								System.out.println(maxOutliers.getEntry(i));
							}

							System.out.println("-----------");

							minOutliers.mapToSelf(new UnivariateFunction() {

								@Override
								public double value(double x) {

									// TP should be negative
									if (x < 0L) {
										return 1L;
									} else
										return 0L;

								}
							});

							maxOutliers.mapToSelf(new UnivariateFunction() {

								@Override
								public double value(double x) {

									// TP should be positive
									if (x > 0L) {
										return 1L;
									} else
										return 0L;
								}
							});

							RealVector outliers = minOutliers.add(maxOutliers);

							System.out.println("Outliers");
							System.out.println("##################");
							System.out.print(v1._1());
							for (int i = 0; i < outliers.getDimension(); i++) {
								System.out.print(" " + outliers.getEntry(i) + " ");
							}
							System.out.println("##################");
							return new Tuple2<String, Vector>(v1._1, Vectors.dense(outliers.toArray()));

						}
					});

					System.out.println(outliers.count());
				}
				return null;
			}
		});

		// Math.sqrt();

		jssc.start();

		jssc.awaitTermination();

	}
}
