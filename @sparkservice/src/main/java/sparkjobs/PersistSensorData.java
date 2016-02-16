package sparkjobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class PersistSensorData {

	public static int i = 0;

	public static void main(String[] args) throws IOException {

		Configuration conf = null;

		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2182");

		Connection connection = ConnectionFactory.createConnection(conf);

		Table testTable = connection.getTable(TableName.valueOf("sensordata"));

		SparkConf sparkConf = new SparkConf().setAppName("spark").setMaster("local");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

		Set<String> topics = new HashSet<>();
		Map<String, String> kafkaParams = new HashMap<>();
		topics.add("test");
		kafkaParams.put("metadata.broker.list", "localhost:9092");

		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		
		messages.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
			
			@Override
			public Void call(JavaPairRDD<String, String> v1) throws Exception {

				List<Put> newRows = new ArrayList<>();

				for (Tuple2<String, String> tuple : v1.collect()) {

					Put put = new Put(Bytes.toBytes("" + i++));
					put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("json"), Bytes.toBytes(tuple._2));
					newRows.add(put);

					System.out.println("Adding row " + i + " with value " + tuple._2);
				}
				testTable.put(newRows);
				System.out.println("Added " + newRows.size() + " rows.");
				return null;
			}
		});

		// Start the computation
		jssc.start();
		jssc.awaitTermination();

	}
	
}
