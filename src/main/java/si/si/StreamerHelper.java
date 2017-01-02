package si.si;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.Authorization;
import twitter4j.conf.ConfigurationBuilder;

public class StreamerHelper {
	private static final String TWITTER_PROPS_FILE = "twitter.properties";
	private static Properties props;

	public static void configureTwitterCredentials()
	throws ClassNotFoundException, IOException {
	props = new Properties();
	InputStream input = StreamerHelper.class.getClassLoader().getResourceAsStream(TWITTER_PROPS_FILE);
	props.load(input);

	}
	public static Authorization getAuthority() {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(props.getProperty("consumerKey").trim())
		.setOAuthConsumerSecret(props.getProperty("consumerSecret").trim())
		.setOAuthAccessToken(props.getProperty("accessToken").trim())
		.setOAuthAccessTokenSecret(props.getProperty("accessTokenSecret").trim());
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		return twitter.getAuthorization();
		}
	
	public static String[] getKeys() {
		return new String[] { "terror", "sci-fi", "drama", "comedia", "accion"};
		}
	
	public static Double getScore(String line) throws Exception {
		Long textLength = 0L;
		int sumOfValues = 0;
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		if (line != null && line.length() > 0) {
			int longest = 0;
			Annotation annotation = pipeline.process(line);
			for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
				Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
				int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
				String partText = sentence.toString();
				if (partText.length() > longest) {
					textLength += partText.length();
					sumOfValues = sumOfValues + sentiment * partText.length();
				}
			}
		}
		return (double) sumOfValues / textLength;
	}

}
