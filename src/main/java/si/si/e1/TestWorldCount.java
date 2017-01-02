package si.si.e1;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public final class TestWorldCount {
	private static final String NAME = "TestWordCount";
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(final String[] args) throws Exception {

		// 1. Definir un SparkContext final
		SparkConf sConfig = new SparkConf().setAppName(NAME).setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(sConfig);

		// 2. Resolver nuestro problema
		JavaRDD<String> lines = ctx.textFile(args[0] + "a.txt");
		JavaRDD<String> words = lines.flatMap(x -> ((List<String>) (Arrays.asList(SPACE.split(x)))).iterator());
		JavaPairRDD<String, Integer> ones = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		JavaPairRDD<String, Integer> counts_sort = counts.sortByKey(true);
		System.out.println("UnSort Map 1 performed: " + counts.collectAsMap() + "\nSort list 1 performed: "
				+ counts_sort.collect());

		// 3. Liberar recursos
		ctx.stop();
		ctx.close();
	}

}
