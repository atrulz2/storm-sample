import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Tuple
import twitter4j.Status

class Printer extends BaseBasicBolt {

	override def execute(input: Tuple, collector: BasicOutputCollector) = println(input)

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = ()

}