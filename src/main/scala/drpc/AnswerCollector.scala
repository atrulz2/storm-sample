package drpc

import scala.collection.mutable.Set

import backtype.storm.coordination.BatchOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBatchBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values

class AnswerCollector extends BaseBatchBolt[Object] {

	private var _collector: BatchOutputCollector = null
	private var _id: Object = null
	private var results: List[String] = null

	override def prepare(conf: java.util.Map[_, _], context: TopologyContext, collector: BatchOutputCollector, id: Object) = {
		_collector = collector
		_id = id
		results = List()
	}

	override def execute(tuple: Tuple) {
		val answers = tuple.getValue(1).asInstanceOf[Set[String]]
		results = results ++ answers
	}

	override def finishBatch() = {
		_collector.emit(new Values(_id, results))
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(new Fields("id", "clear"))
}