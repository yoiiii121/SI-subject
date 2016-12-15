package si.si.e3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import si.si.SparkConfigs;

public final class TestManagementDB_Join_JSON {
	private static final String NAME = "TestWordCount";
	private static String[] paths = { "./src/main/java/si/si/e3/a.json", "./src/main/java/si/si/e3/b.json"};

	public static void main(String[] args) {
		String master = System.getProperty("spark master");
		SparkConf sc = SparkConfigs.create(NAME, master == null ? "local" : master);

		JavaSparkContext ctx = new JavaSparkContext(sc);
		SQLContext sql = SQLContext.getOrCreate(ctx.sc());

		Dataset<Row> dataset = sql.read().option("inferShchema", true).json(paths[0]);
		Dataset<Row> dataset2 = sql.read().option("inferShchema", true).json(paths[1]);

		Dataset<Row> join = dataset.join(dataset2);

		join.show();

		ctx.stop();
		ctx.close();
	}
}
