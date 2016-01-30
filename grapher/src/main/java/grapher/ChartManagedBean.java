package grapher;

import java.io.IOException;
import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;

import org.codehaus.jackson.map.ObjectMapper;
import org.primefaces.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ManagedBean
@SessionScoped
public class ChartManagedBean implements Serializable {

	private static final long serialVersionUID = 1L;
	Logger logger = LoggerFactory.getLogger(ChartManagedBean.class);

	private static final ObjectMapper objectMapper = new ObjectMapper();

	@PostConstruct
	public void initialize() {
		logger.info("Initializing bean");
		ReadHbaseData.initDataReading();
	}

	public void prepareChartData() throws IOException {
		logger.info("Preparing chart");
		// Produce you JSON string (I use Gson here)

		String jsonString = ReadHbaseData.getChartData();

		RequestContext reqCtx = RequestContext.getCurrentInstance();
		reqCtx.addCallbackParam("chartData", jsonString);
	}

}