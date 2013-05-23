import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.task.TopologyContext
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.tuple.Tuple
import scala.io.Source
import twitter4j.Status
import backtype.storm.tuple.Values
import backtype.storm.tuple.Fields
import backtype.storm.topology.base.BaseBasicBolt

class SentimentProbScorer extends BaseBasicBolt {

	private var sentimentDictionary: Map[String, (Double, Double)] = null

	/**
	 * score the tweet using AFINN-111
	 *
	 */
	override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext) {
		val lines = Source.fromURL(getClass.getResource("/twitter_sentiment_list.csv"), "UTF-8").getLines.toList
		sentimentDictionary = lines.map(line => {
			val tokens = line.split(",")
			(tokens(0), (tokens(1).toDouble, tokens(2).toDouble))
		})(collection.breakOut)
	}

	/**
	 * pick out all words in the tweet text and score any word in the dictionary
	 *
	 */
	override def execute(input: Tuple, collector: BasicOutputCollector) = {
		val tweet = input.getValue(0).asInstanceOf[Status]
		// get the happy/sad log probabilities for the entire tweet
		val logProbs = for {
			word <- "[\\w:\\.\\?]+".r findAllIn tweet.getText
			probabilities = sentimentDictionary.getOrElse(word.toLowerCase, null)
			if probabilities != null
		} yield probabilities
		val (happy, sad) = logProbs.foldLeft((0.0, 0.0))((t1, t2) => (t1._1 + t2._1, t2._2 + t2._2))
		if (happy != 0.0 || sad != 0.0) {
			collector.emit(new Values(tweet.getText, if (happy > sad) "happy" else "sad" ))
		}
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(new Fields("tweet", "sentiment"))
}