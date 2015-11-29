package sparkstreamer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class ReadFromKafkaWriteToHbase {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws IOException {

		Configuration conf = null;

		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2182");

		Connection connection = ConnectionFactory.createConnection(conf);

		Table testTable = connection.getTable(TableName.valueOf("test"));

		Get g = new Get(Bytes.toBytes("row5"));
		Result r = testTable.get(g);

		System.out.println(r);

		// ----------------------------------//

		SparkConf sparkConf = new SparkConf().setAppName("spark").setMaster("local");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

		Set<String> topics = new HashSet<>();
		Map<String, String> kafkaParams = new HashMap<>();
		topics.add("test");
		kafkaParams.put("metadata.broker.list", "localhost:9092");

		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		JavaDStream<String> lines = messages.map(new Function<String,String>() {
			@Override
			public String call(String a) {
				return a;
			}
		});

		for (String string : words) {
			Put put = new Put(Bytes.toBytes("row6"));
			put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes("bau"));
			testTable.put(put);
		}

		// Start the computation
		jssc.start();
		jssc.awaitTermination();

	}

}
