package grapher;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class JavaSample {

	public static void main(String[] args) {
		String farm = "f1";
		String greenhouse = "g1";

		List<String> timestamps = new ArrayList<>();

		timestamps.add("2015-09-04_12:23:48");
		timestamps.add("2015-09-06_17:44:53");
		timestamps.add("2015-09-08_20:44:57");
		timestamps.add("2015-10-07_16:30:00");

		timestamps.forEach(t -> {
			DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY-MM-dd_HH:mm:ss");
			DateTime dt = formatter.parseDateTime(t);
			System.out.println(farm + greenhouse + "_" + dt.getMillis());

			// System.out.println(new DateTime(dt.getMillis()));
		});
		
		Integer[] bau = {1,2,3};
		
	
	}

}
