package grapher;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;

import org.primefaces.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ManagedBean
@SessionScoped
public class ChartManagedBean implements Serializable {

	private static final long serialVersionUID = 1L;
	Logger logger = LoggerFactory.getLogger(ChartManagedBean.class);

	@PostConstruct
	public void initialize() {
		logger.info("Initializing bean");
		ReadHbaseData.initDataReading();
	}

	public void prepareChartData() throws IOException {
		logger.info("Preparing chart");

		Map<String, String> chartData = ReadHbaseData.getChartData();
		RequestContext reqCtx = RequestContext.getCurrentInstance();

		reqCtx.addCallbackParam("timestamps", chartData.get("timestamps"));
		reqCtx.addCallbackParam("int_temp", chartData.get("int_temp"));
		reqCtx.addCallbackParam("timestamps_outliers", chartData.get("timestamps_outliers"));
		reqCtx.addCallbackParam("int_temp_out", chartData.get("int_temp_out"));
	}
}
