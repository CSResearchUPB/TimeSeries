package serializers;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;

import kafka.utils.VerifiableProperties;

public class JsonDecoder implements Deserializer<Object> {

	private static final ObjectMapper objectMapper = new ObjectMapper();
	
	public JsonDecoder() {
	}

	public JsonDecoder(VerifiableProperties verifiableProperties) {
		/* This constructor must be present for successful compile. */
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object deserialize(String topic, byte[] data) {
		try {
			return objectMapper.readValue(data, JSONObject.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
