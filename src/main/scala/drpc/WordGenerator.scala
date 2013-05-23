package drpc

import backtype.storm.task.TopologyContext
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values
import scala.util.parsing.json.JSON
import scala.util.parsing.json.JSONObject

class WordGenerator extends BaseBasicBolt {

	override def prepare(stormConf: java.util.Map[_, _], context: TopologyContext) = ()

	override def execute(input: Tuple, collector: BasicOutputCollector) = {
		val id = input.getValue(0)
		val arg = input.getString(1)
		// arg is json encoded, parse out into ciphertext and length
		val json = JSON.parseRaw(arg).get.asInstanceOf[JSONObject]
		val words = generateWords(json.obj("length").asInstanceOf[Double].toInt)
		val cipher = hex2Bytes(json.obj("cipher").asInstanceOf[String])
		for (word <- words) {
			collector.emit(new Values(id, cipher, word))
		}
	}
	
	def hex2Bytes(hex: String): Array[Byte] = {
		(for {
			i <- 0 to hex.length - 1 by 2
			if i > 0 || !hex.startsWith("0x")
		} yield hex.substring(i, i + 2)).map(Integer.parseInt(_, 16).toByte).toArray
	}
	
	def generateWords(len: Int): Set[String] = {
		if (len == 0) Set("")
		else for {
			word <- generateWords(len - 1)
			c <- 'a' to 'z'
		} yield c + word
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(new Fields("id", "cipher", "clear"))
}