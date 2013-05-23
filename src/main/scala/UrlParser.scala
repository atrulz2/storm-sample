import backtype.storm.task.TopologyContext
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values
import twitter4j.Status

class UrlParser extends BaseBasicBolt {

	/**
	 * score the tweet using AFINN-111
	 *
	 */
	override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext) = ()

	/**
	 * pick out all words in the tweet text and score any word in the dictionary
	 *
	 */
	override def execute(input: Tuple, collector: BasicOutputCollector) = {
		val tweet = input.getValue(0).asInstanceOf[Status]
		for (url <- tweet.getURLEntities)
			collector.emit(new Values(url.getDisplayURL))
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(new Fields("url"))
}