import scala.collection.breakOut
import scala.collection.immutable.Map
import backtype.storm.task.TopologyContext
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values
import twitter4j.Status
import scala.io.Source

class SentimentScorer extends BaseBasicBolt {

	private var sentimentDictionary: Map[String, Int] = null

	/**
	 * score the tweet using AFINN-111
	 *
	 */
	override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext) {
		val lines = Source.fromURL(getClass.getResource("/AFINN-111.txt"), "UTF-8").getLines.toList
		sentimentDictionary = lines.map(line => {
			val tokens = line.split("\t")
			(tokens(0), tokens(1).toInt)
		})(breakOut)
	}

	/**
	 * pick out all words in the tweet text and score any word in the dictionary
	 *
	 */
	override def execute(input: Tuple, collector: BasicOutputCollector) = {
		val tweet = input.getValue(0).asInstanceOf[Status]
		// parse out all the words in the tweet
		val score = (for (word <- "[\\w]+".r findAllIn tweet.getText) yield sentimentDictionary.getOrElse(word.toLowerCase, 0)).sum
		collector.emit(new Values(tweet.getText, if (score > 0) "happy" else if (score < 0) "sad" else "neutral"))
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(new Fields("tweet", "sentiment"))
}