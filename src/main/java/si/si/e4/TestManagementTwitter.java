package si.si.e4;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import si.si.SparkConfigs;
import si.si.StreamerHelper;
import twitter4j.Status;
import twitter4j.auth.Authorization;

public final class TestManagementTwitter {

	public static void main(String[] args) throws Exception {
		// 1. Definir el objeto configurador de Spark
		String master = System.getProperty("spark.master");
		JavaSparkContext ctx = new JavaSparkContext(
				SparkConfigs.create("StreamingTwitter", master == null ? "local[2]" : master));
		ctx.setLogLevel("WARN");
		// 2. Twitter credentials from twitter.properties
		StreamerHelper.configureTwitterCredentials();
		Authorization twitter = StreamerHelper.getAuthority();
		String[] filters = StreamerHelper.getKeys();
		JavaStreamingContext ssc = new JavaStreamingContext(ctx, new Duration(10000));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc, twitter, filters);
		JavaDStream<String> filtered = stream.map(status -> "" + status.getText());
		filtered.foreachRDD(rdd -> {
			LocalDateTime lc = LocalDateTime.now();
			System.out.println(rdd.collect());
			String sSistemaOperativo = System.getProperty("os.name");

			if (sSistemaOperativo.contains("Windows")) {

				rdd.saveAsTextFile(args[1] + Path.SEPARATOR + lc.toEpochSecond(ZoneOffset.UTC));

			} else {
				rdd.saveAsTextFile(args[0] + Path.SEPARATOR + lc.toEpochSecond(ZoneOffset.UTC));
			}
		});
		// 3. Abrir canal de datos
		ssc.start();
		ssc.awaitTermination();
	}

}