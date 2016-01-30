package grapher;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadHbaseData {

	public static Table sensors;

	public static void initDataReading() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2182");

		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			sensors = connection.getTable(TableName.valueOf("sensors"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String getChartData() {

		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes("f1g1_1441362228000"));
		scan.setStopRow(Bytes.toBytes("f1g1_1444228200000"));

		ResultScanner rs = null;
		try {
			rs = sensors.getScanner(scan);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			for (Result r = rs.next(); r != null; r = rs.next()) {
				System.out.println(r.toString());

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			rs.close(); // always close the ResultScanner!
		}
		try {
			sensors.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String result = "{ x: [1, 2, 3]}";

		return result;

	}

	public static void main(String[] args) {
		initDataReading();
		getChartData();
	}

}
