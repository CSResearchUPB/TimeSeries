package grapher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import org.joda.time.DateTime;

import com.google.gson.Gson;

public class ReadHbaseData {

	public static Table sensors;

	private static final Gson GSON = new Gson();

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
			sensors = connection.getTable(TableName.valueOf("sensortest2"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Map<String, String> getChartData() {

		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes("f1g1_1441362228000"));
		// scan.setStopRow(Bytes.toBytes("f1g1_1444228200000"));

		List<String> timestamps = new ArrayList<>();
		List<String> timestampsOutliers = new ArrayList<>();
		List<Double> values = new ArrayList<>();
		List<Double> outliers = new ArrayList<>();

		ResultScanner rs = null;
		try {
			rs = sensors.getScanner(scan);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			for (Result r = rs.next(); r != null; r = rs.next()) {
				String t = new DateTime(new Long(Bytes.toString(r.getRow()).split("_")[1]))
						.toString("dd-MM-YYYY HH:mm:ss");
				timestamps.add(t);
				Double v = new Double(Bytes.toString(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("it"))));
				values.add(v);

				Double o = new Double(Bytes.toString(r.getValue(Bytes.toBytes("o"), Bytes.toBytes("it"))));
				if (!o.equals(0.0)) {
					outliers.add(o);
					timestampsOutliers.add(t);
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			rs.close();
		}
		try {
			sensors.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<Double> outliers2 = outliers.stream().map(o -> {
			if (o.equals(0.0)) {
				return 100.0;
			}
			return o;
		}).collect(Collectors.toList());

		String timestampJSon = GSON.toJson(timestamps);
		String timestampOutliersJson = GSON.toJson(timestampsOutliers);
		String valuesJson = GSON.toJson(values);
		String outliersJson = GSON.toJson(outliers2);

		Map<String, String> chartData = new HashMap<>();

		chartData.put("timestamps", timestampJSon);
		chartData.put("int_temp", valuesJson);
		chartData.put("timestamps_outliers", timestampOutliersJson);
		chartData.put("int_temp_out", outliersJson);

		System.out.println(chartData);

		return chartData;

	}

	public static void main(String[] args) {
		initDataReading();
		getChartData();
	}

}
