import backtype.storm.task.TopologyContext
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values

class WordFilter(stopWords: Array[String]) extends BaseBasicBolt {

	override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext) = ()

	override def execute(input: Tuple, collector: BasicOutputCollector) = {
		val word = input.getString(0)
		if (!stopWords.contains(word))
			collector.emit(new Values(word))
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(new Fields("word"))
}