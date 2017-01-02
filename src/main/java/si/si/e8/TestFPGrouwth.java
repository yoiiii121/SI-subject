package si.si.e8;

import java.util.Arrays;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import si.si.SparkConfigs;

public class TestFPGrouwth {

	private static final Double MIN_SUPPORT = 0.3;
	private static final Integer NUM_PARTITIONS = 1000;
	private static final String NAME = "TestFPGrouwth";
	private static final String URL = "jdbc:mysql://localhost:3306/films";
	private static final String TABLE = "ratingsgrowth";

	public static void main(String[] args) throws Exception {

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
		properties.setProperty("useLegacyDatetimeCode", "false");
		properties.setProperty("serverTimezone", "UTC");
		Dataset<Row> completa = sql.read().jdbc(URL, TABLE, properties);
		
		
		JavaRDD<Iterable<String>> aux = completa.javaRDD()
				.mapToPair(x -> new Tuple2<String, String>("" + x.getInt(0), "" + x.getInt(1)))
				.reduceByKey((z, y) -> (String) (z + " " + y))
				.map(x -> Arrays.asList(x._2.split(" ")));
		FPGrowth fp = new FPGrowth().setMinSupport(MIN_SUPPORT).setNumPartitions(NUM_PARTITIONS);
		FPGrowthModel<String> model = fp.run(aux);
		for (

		FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
			System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
		}
		ctx.stop();
		ctx.close();
	}
}
