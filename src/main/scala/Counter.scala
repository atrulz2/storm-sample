import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.Tuple
import scala.collection.mutable.Map

class Counter(interval: Int) extends TimedBolt(interval) {

	private val counters: Map[String, Int] = Map()

	override def executeTimerTask() = {
		println(counters.toList.sortBy(-_._2))
		counters.clear
	}
	override def processTuple(input: Tuple) = {
		val key = input.getString(0)
		counters.update(key, counters.getOrElse(key, 0) + 1)
	}
	override def declareOutputFields(declarer: OutputFieldsDeclarer) = ()

}