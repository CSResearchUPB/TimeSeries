package kafkajobs;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import serializers.JsonEncoder;

public class ProduceOnlineSensorData {

	public static void main(String[] args) throws IOException {

		List<File> dataset = Files.list(Paths.get("/root/Desktop/datasets/")).map(fp -> fp.toFile())
				.collect(Collectors.toList());

		Collections.sort(dataset, new Comparator<File>() {

			@Override
			public int compare(File file1, File file2) {
				return new Integer(file1.getName().substring(5))
						.compareTo(Integer.parseInt(file2.getName().substring(5)));
			}
		});

		JSONParser parser = new JSONParser();

		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", StringSerializer.class);
		props.put("value.serializer", JsonEncoder.class);

		KafkaProducer<String, JSONObject> producer = new KafkaProducer<String, JSONObject>(props);

		for (File file : dataset) {
			Object obj = null;
			try {
				obj = parser.parse(new FileReader(file));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			JSONObject jsonObject = (JSONObject) obj;

			ProducerRecord<String, JSONObject> message = new ProducerRecord<String, JSONObject>("test", 0,
					"farmsensors", jsonObject);
			producer.send(message);
			System.out.println("Sent message" + message);

			try {
				Thread.sleep(1000);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}

		producer.close();

	}

}
