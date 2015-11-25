import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SensorDataKafkaProducer {

	public static void main(String[] args) {

		Properties props = new Properties();

		props.put("bootstrap.servers", "192.168.0.114:9092");
		props.put("key.serializer", StringSerializer.class);
		props.put("value.serializer", StringSerializer.class);

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
				props);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(
				"test", "java remote test 3");

		producer.send(record);

		System.out.println("Sent message" + record);

		producer.close();

	}

}
