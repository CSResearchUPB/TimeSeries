package sparkjobs;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ProcessHistoricalData {

	public static int i = 0;

	public static void main(String[] args) throws IOException {

		Configuration conf = null;

		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2182");

		Connection connection = ConnectionFactory.createConnection(conf);

		Table testTable = connection.getTable(TableName.valueOf("sensordata"));

		SparkConf sparkConf = new SparkConf().setAppName("spark").setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		
		
		
		jsc.stop();

	}

}
