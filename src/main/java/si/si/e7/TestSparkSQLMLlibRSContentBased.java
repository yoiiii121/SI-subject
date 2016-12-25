package si.si.e7;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import si.si.SparkConfigs;

public final class TestSparkSQLMLlibRSContentBased {
	private static final String NAME = "TestSparkSQLMLlibRSContentBased";
	private static final String URL = "jdbc:mysql://localhost:3306/films";
	private static final String TABLE = "ratingusersitems";

	public static void main(String[] args) throws AnalysisException {
		String master = System.getProperty("spark.master");
		JavaSparkContext ctx = new JavaSparkContext(SparkConfigs.create(NAME, master == null ? "local[2]" : master)
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

		JavaRDD<RatioCB> rdd = dataset.select(dataset.col("idUser").as("user1"), dataset.col("idMovie").as("film1"))
				.join(dataset.select(dataset.col("idUser").as("user2"), dataset.col("idMovie").as("film2")))
				.where("user1 = user2").where("film1 <> film2").select("film1", "film2").javaRDD()
				.mapToPair(x -> new Tuple2<Row, Integer>(x, 1)).reduceByKey((x, y) -> x + y).map(x -> new RatioCB(x));

		sql.createDataFrame(rdd, RatioCB.class).write().jdbc(URL, TABLE + "CB", properties);

		String sSistemaOperativo = System.getProperty("os.name");

		LocalDateTime lc = LocalDateTime.now();

		if (sSistemaOperativo.contains("Windows")) {
			rdd.saveAsTextFile(args[0] + "Filtered" + Path.SEPARATOR + lc.toEpochSecond(ZoneOffset.UTC));

		} else {
			rdd.saveAsTextFile(args[0] + "Filtered" + Path.SEPARATOR + lc.toEpochSecond(ZoneOffset.UTC));
		}
		ctx.stop();
		ctx.close();
	}
}