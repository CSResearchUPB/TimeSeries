package kafkajobs;

import java.io.File;
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

public class ProduceOnlineSensorData {

	public static void main(String[] args) throws IOException {

		List<File> dataset = Files.list(Paths.get("/root/Desktop/datasets/")).map(fp -> fp.toFile())
				.collect(Collectors.toList());

		Collections.sort(dataset, new Comparator<File>() {

			@Override
			public int compare(File file1, File file2) {
				return new Long(file1.lastModified()).compareTo(new Long(file2.lastModified()));
			}
		});

		for (File file : dataset) {
			System.out.println(file.getName());
		}

		if (true) {
			return;
		}

		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", StringSerializer.class);
		props.put("value.serializer", StringSerializer.class);

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "java remote test 6");

		producer.send(record);

		System.out.println("Sent message" + record);

		producer.close();

	}

}
