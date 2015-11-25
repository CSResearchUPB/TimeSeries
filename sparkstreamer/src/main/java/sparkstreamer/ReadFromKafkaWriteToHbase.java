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

	}

}
