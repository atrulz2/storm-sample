package drpc

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.LocalDRPC
import backtype.storm.drpc.LinearDRPCTopologyBuilder
import backtype.storm.tuple.Fields

object DRPCTopology {

	//	def singlethread() {
	//		val start = System.currentTimeMillis
	//		val words = new WordGenerator().generateWords(5)
	//		val matcher = new SHA256Matcher
	//		matcher.prepare(null, null, null, null)
	//		for (word <- words)
	//			matcher.matches("9a105749cfcbac2e07d7640697861f62dfcc7c02bc24519e9c287431bc493f9d", word)
	//		println("computation took: " + (System.currentTimeMillis - start) + " ms")
	//	}

	def main(args: Array[String]): Unit = {
		//singlethread
		val builder = new LinearDRPCTopologyBuilder("crack")
		builder.addBolt(new WordGenerator, 1)
		builder.addBolt(new SHA256Matcher, 3).shuffleGrouping()
		builder.addBolt(new AnswerCollector, 1).fieldsGrouping(new Fields("id"))

		val drpc = new LocalDRPC
		val cluster = new LocalCluster
		val conf = new Config
		conf.setMessageTimeoutSecs(120)

		cluster.submitTopology("demo", conf, builder.createLocalTopology(drpc))

		val start = System.currentTimeMillis
		println("Results: " + drpc.execute("crack", "{\"cipher\":\"9a105749cfcbac2e07d7640697861f62dfcc7c02bc24519e9c287431bc493f9d\", \"length\": 5}"))
		println("computation took: " + (System.currentTimeMillis - start) + " ms")

		cluster.shutdown
		drpc.shutdown
	}
}