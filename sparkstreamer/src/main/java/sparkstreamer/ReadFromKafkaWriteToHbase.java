package sparkstreamer;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadFromKafkaWriteToHbase {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws IOException {

		Configuration conf = null;

		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.0.120");

		Connection connection = ConnectionFactory.createConnection(conf);

		Table tables = connection.getTable(TableName.valueOf("test"));

		Get g = new Get(Bytes.toBytes("row1"));
		Result r = tables.get(g);

		System.out.println(r);

		// byte[] value = r.getValue(Bytes.toBytes("cf:a"),
		// Bytes.toBytes("someQualifier"));
		//
		// // If we convert the value bytes, we should get back 'Some
		// // Value', the
		// // value we inserted at this location.
		// String valueStr = Bytes.toString(value);
		// System.out.println("GET: " + valueStr);

		// SparkConf conf = new
		// SparkConf().setAppName("spark").setMaster("local");
		//
		// JavaStreamingContext jssc = new JavaStreamingContext(conf,
		// Durations.seconds(10));
		//
		// System.out.println("Running spark");
		//
		// Set<String> topics = new HashSet<>();
		// Map<String, String> kafkaParams = new HashMap<>();
		// topics.add("test");
		// kafkaParams.put("metadata.broker.list", "192.168.0.118:9092");
		//
		// JavaPairInputDStream<String, String> messages =
		// KafkaUtils.createDirectStream(jssc, String.class, String.class,
		// StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		//
		// JavaDStream<String> lines = messages.map(new Function<Tuple2<String,
		// String>, String>() {
		// @Override
		// public String call(Tuple2<String, String> tuple2) {
		// return tuple2._2();
		// }
		// });
		// JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String,
		// String>() {
		// @Override
		// public Iterable<String> call(String x) {
		// return Lists.newArrayList(SPACE.split(x));
		// }
		// });
		// JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new
		// PairFunction<String, String, Integer>() {
		// @Override
		// public Tuple2<String, Integer> call(String s) {
		// return new Tuple2<String, Integer>(s, 1);
		// }
		// }).reduceByKey(new Function2<Integer, Integer, Integer>() {
		// @Override
		// public Integer call(Integer i1, Integer i2) {
		// return i1 + i2;
		// }
		// });
		// wordCounts.print();
		//
		// // Start the computation
		// jssc.start();
		// jssc.awaitTermination();

	}

}
