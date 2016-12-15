package si.si.e2;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import si.si.SparkConfigs;

import org.apache.spark.sql.Row;

public final class TestManagementDB_SQL {
	private static final String NAME = "TestWordCount";

	public static void main(String[] args) {

		// 1. Definir un SparkContext
		String master = System.getProperty("spark.master");
		JavaSparkContext ctx = new JavaSparkContext(SparkConfigs.create(NAME, master == null ? "local" : master));
		SQLContext sql = SQLContext.getOrCreate(ctx.sc());
		// 2. Resolver nuestro problema
		Dataset<Row> dataset = sql.read().option("header", true).option("inferSchema", true)
				.csv("./src/main/java/si/si/e2/a.csv");
		Dataset<Row> manufacturas = dataset.select(dataset.col("Manufacturer")).distinct();

		dataset.printSchema();
		manufacturas.show();

		// 3. Liberar recursos
		ctx.stop();
		ctx.close();
	}
}
