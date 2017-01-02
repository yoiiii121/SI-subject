package si.si.e6;


import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import si.si.SparkConfigs;

public final class TestSparkSQLMLlibRSALS {
	private static final String NAME = "TestSparkSQLMLlibRSALS";
	private static final String URL = "jdbc:mysql://localhost:3306/films";
	private static final String TABLE = "items";

	public static void main(String[] args) throws AnalysisException {
		String master = System.getProperty("spark.master");
		JavaSparkContext ctx = new JavaSparkContext(SparkConfigs.create(NAME, master == null ? "local[3]" : master)
				.set("spark.sql.crossJoin.enabled", "true"));

		SQLContext sql = SQLContext.getOrCreate(ctx.sc());
		Properties properties = new Properties();
		properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
		properties.setProperty("user", "root");
		properties.setProperty("password", "");
		properties.setProperty("allowMultiQueries", "true");
		properties.setProperty("rewriteBatchedStatements", "true");
		properties.setProperty("useUnicode", "true");
		properties.setProperty("useJDBCCompliantTimezoneShift", "true");
		properties.setProperty("use‌​LegacyDatetimeCode", "false");
		properties.setProperty("serverTimezone", "UTC");

		Dataset<Row> dataset = sql.read().jdbc(URL, TABLE, properties);
		ALS als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("idUser").setItemCol("idMovie")
				.setRatingCol("rating");
		ALSModel model = als.fit(dataset);
		Dataset<Row> completa = dataset.select(dataset.col("idUser")).distinct()
				.join(dataset.select(dataset.col("idMovie")).distinct());
		completa = model.transform(completa);
		completa.write().jdbc(URL, TABLE + "ALS", properties);
		ctx.stop();
		ctx.close();

	}
}