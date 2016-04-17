package sparkjobs;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.PolynomialExpansion;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class ProcessHistoricalData {

	public static int i = 0;

	public static void main(String[] args) throws IOException {

		SparkConf sparkConf = new SparkConf().setAppName("spark").setMaster("local");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(jsc);

		// $example on$
		// Load training data
		DataFrame training = sqlContext.read().format("libsvm").load("src/main/resources/lr_sample.txt");
		DataFrame test = sqlContext.read().format("libsvm").load("src/main/resources/lr_sample_test.txt");

		training.show(false);
		training.printSchema();

//		MinMaxScaler scaler = new MinMaxScaler()
//		  .setInputCol("features")
//		  .setOutputCol("scaledFeatures");
//
//		// Compute summary statistics and generate MinMaxScalerModel
//		MinMaxScalerModel scalerModel = scaler.fit(training);
//
//		// rescale each feature to range [min, max].
//		DataFrame scaledData = scalerModel.transform(training);
//		scaledData.show(false);
//		
//		PCAModel pca = new PCA()
//				  .setInputCol("scaledFeatures")
//				  .setOutputCol("pcaFeatures")
//				  .setK(5)
//				  .fit(scaledData);
//
//		DataFrame pcaResults = pca.transform(scaledData);
//		pcaResults.show(false);
//		
		PolynomialExpansion polyExpansion = new PolynomialExpansion()
				  .setInputCol("features")
				  .setOutputCol("polyfeatures")
				  .setDegree(2);
		
		DataFrame polyDFTraining = polyExpansion.transform(training);
		
		polyDFTraining.show(false);
		
		LinearRegression lr = new LinearRegression().setMaxIter(100);//.setRegParam(0.3).setElasticNetParam(0.8);

		// Fit the model
		
		
		LinearRegressionModel lrModel = lr.setFeaturesCol("polyfeatures").fit(polyDFTraining);

		// Print the coefficients and intercept for linear regression
		System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

		// Summarize the model over the training set and print out some metrics
		LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
		System.out.println("numIterations: " + trainingSummary.totalIterations());
		System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
		trainingSummary.residuals().show();
		System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
		System.out.println("r2: " + trainingSummary.r2());
		// $example off$
		System.out.println(lrModel.explainParams());

		System.out.println("Predicting on training data...");
		lrModel.setFeaturesCol("polyfeatures").transform(polyDFTraining).show(2000,false);
		
		DataFrame polyDFTest = polyExpansion.transform(test);
		System.out.println("Predicting on test data...");
		lrModel.setFeaturesCol("polyfeatures").transform(polyDFTest).show(1000,false);

		jsc.stop();

	}

}
