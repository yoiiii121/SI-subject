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
	
	private static final String PATHLinux = "/home/nu14/Escritorio/Twitts";
	private static final String PATHWindows = "C:\\Users\\Nacho\\Desktop\\Twitts";
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
		JavaStreamingContext ssc = new JavaStreamingContext(ctx, new Duration(100000));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc, twitter, filters);
		JavaDStream<String> filtered = stream.map(status -> "" + status.getText());
		String sSistemaOperativo = System.getProperty("os.name");
		filtered.foreachRDD(rdd -> {
			LocalDateTime lc = LocalDateTime.now();
			System.out.println(rdd.collect());
			
			if (sSistemaOperativo.contains("Windows")) {
			
			 rdd.saveAsTextFile(PATHWindows + Path.SEPARATOR + lc.toEpochSecond(ZoneOffset.UTC));

			}else{
				rdd.saveAsTextFile(PATHLinux + Path.SEPARATOR + lc.toEpochSecond(ZoneOffset.UTC));
			}

		});
		// 3. Abrir canal de datos
		ssc.start();
		ssc.awaitTermination();
	}

}