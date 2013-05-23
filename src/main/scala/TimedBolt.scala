import java.util.{Map => JMap}

import scala.collection.JavaConverters.mapAsJavaMapConverter

import backtype.storm.Config
import backtype.storm.Constants
import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple

abstract class TimedBolt(interval: Int) extends BaseRichBolt {

	protected var collector: OutputCollector = null

	override def prepare(stormConf: JMap[_, _], context: TopologyContext, collector: OutputCollector) = {
		this.collector = collector
	}

	override def execute(input: Tuple) = {
		if (input.getSourceStreamId == Constants.SYSTEM_TICK_STREAM_ID) executeTimerTask
		else processTuple(input)
		collector.ack(input)
	}

	override def getComponentConfiguration: JMap[String, Object] = Map(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS -> interval.asInstanceOf[java.lang.Object]).asJava

	def executeTimerTask()
	def processTuple(input: Tuple)
}