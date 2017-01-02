package si.si.e5;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.mongodb.spark.MongoSpark;

import scala.Tuple2;
import si.si.SparkConfigs;
import si.si.StreamerHelper;

public final class TestTextSentimentTwitter {

	public static void main(String[] args) throws Exception {
		// 1. Definir el objeto configurador de Spark
		String master = System.getProperty("spark.master");
		JavaSparkContext ctx = new JavaSparkContext(
				SparkConfigs.create("StreamingTwitter", master == null ? "local[2]" : master));
		ctx.setLogLevel("WARN");

		JavaStreamingContext ssc = new JavaStreamingContext(ctx, new Duration(10000));
		String sSistemaOperativo = System.getProperty("os.name");

		JavaDStream<String> stream = null;
		if (sSistemaOperativo.contains("Windows")) {

			stream = ssc.textFileStream(args[1]);

		} else {
			stream = ssc.textFileStream(args[0]);
		}

		SQLContext sql = SQLContext.getOrCreate(ctx.sc());
		// 2. Resolver nuestro problema
		stream.foreachRDD(rdd -> {
			JavaRDD<Tuple2<String, Double>> data = rdd
					.map(x -> new Tuple2<String, Double>(x, StreamerHelper.getScore(x)));
			Dataset<Tuple2<String, Double>> dataset = sql.createDataset(data.rdd(),
					Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()));
			MongoSpark.save(dataset);
		});
		// 3. Abrir canal de datos
		ssc.start();
		ssc.awaitTermination();
		ssc.close();
	}

}
