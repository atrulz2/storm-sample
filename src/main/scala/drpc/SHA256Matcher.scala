package drpc

import backtype.storm.coordination.BatchOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBatchBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values
import java.security.MessageDigest
import scala.collection.mutable.Set

class SHA256Matcher extends BaseBatchBolt[Object] {

	private var _collector: BatchOutputCollector = null
	private var _id: Object = null
	private var results: Set[String] = null
	@transient private var sha: MessageDigest = null

	override def prepare(conf: java.util.Map[_, _], context: TopologyContext, collector: BatchOutputCollector, id: Object) = {
		_collector = collector
		_id = id
		sha = MessageDigest.getInstance("SHA-256")
		results = Set()
	}

	override def execute(tuple: Tuple) {
		val cipher = tuple.getValue(1).asInstanceOf[Array[Byte]]
		val clear = tuple.getString(2)
		if (matches(cipher, clear))
			results.add(clear)
	}
	
	def matches(cipher: Array[Byte], clear: String) = {
		sha.update(clear.getBytes)
		sha.digest.corresponds(cipher)(_==_)
	}

	override def finishBatch() = {
		_collector.emit(new Values(_id, results))
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(new Fields("id", "clear"))
}