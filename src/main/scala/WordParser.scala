import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.task.TopologyContext
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.tuple.Tuple
import twitter4j.Status
import backtype.storm.tuple.Values
import backtype.storm.tuple.Fields

class WordParser extends BaseBasicBolt {

	override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext) = ()

	override def execute(input: Tuple, collector: BasicOutputCollector) = {
		val tweet = input.getValue(0).asInstanceOf[Status]
		for (word <- "[\\w]+".r findAllIn tweet.getText)
			collector.emit(new Values(word))
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(new Fields("word"))
}