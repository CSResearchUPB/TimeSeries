package serializers;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import kafka.utils.VerifiableProperties;

public class JsonEncoder implements Serializer<Object> {
	// instantiating ObjectMapper is expensive. In real life, prefer injecting
	// the value.
	private static final ObjectMapper objectMapper = new ObjectMapper();

	public JsonEncoder() {
	}

	public JsonEncoder(VerifiableProperties verifiableProperties) {
		/* This constructor must be present for successful compile. */
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] serialize(String topic, Object data) {
		try {
			return objectMapper.writeValueAsString(data).getBytes();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "".getBytes();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
}
