import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.task.TopologyContext
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.tuple.Tuple
import scala.collection.mutable.Map

class SentimentAccumulator extends BaseBasicBolt {
	val counter = Map("happy" -> 0, "sad" -> 0, "neutral" -> 0)
	override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext) = ()
	override def execute(input: Tuple, collector: BasicOutputCollector) = {
		counter(input.getString(1)) = counter.get(input.getString(1)).get + 1
		println(counter)
	}
	override def declareOutputFields(declarer: OutputFieldsDeclarer) = ()

}