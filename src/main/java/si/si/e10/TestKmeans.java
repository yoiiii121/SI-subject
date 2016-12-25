package si.si.e10;

import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


import si.si.SparkConfigs;

public class TestKmeans {
	private static final String NAME = "TestSparkSQLMLlibRSContentBased";
	private static final String URL = "jdbc:mysql://localhost:3306/films";
	private static final String TABLE = "users";// USERS
	private static final Integer MAX_ITER = 10;
	private static final Integer SEED = 100;
	private static final Integer K = 5;
	

	public static void main(String[] args) {
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

		Dataset<Row> users = sql.read().jdbc(URL, TABLE, properties);
		
		JavaRDD<Vector> jrdd = new JavaRDD<Row>(users.rdd(),
				users.org$apache$spark$sql$Dataset$$classTag()).
				map(x -> Vectors.dense(new double[] { x.getInt(0) }));
		// Cluster the data into two classes using KMeans
		KMeansModel clusters = new KMeans().setSeed(SEED).setK(K).setMaxIterations(MAX_ITER).run(jrdd.rdd());
	System.out.println(clusters.clusterCenters()[0]);
	System.out.println(clusters.clusterCenters()[1]);
		
		ctx.stop();
		ctx.close();
	}
	

}
