import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder

object SampleTopology {

	def main(args: Array[String]): Unit = {
		val kw = List("obama", "barack", "potus", "white house")
		val builder = new TopologyBuilder
		builder.setSpout("spout.twitter", new TwitterStreamingSpout(kw), 1)
		builder.setBolt("bolt.hashtag.parser", new HashtagParser, 4).shuffleGrouping("spout.twitter")
		builder.setBolt("bolt.hashtag.counter", new Counter(10), 1).shuffleGrouping("bolt.hashtag.parser")
		builder.setBolt("bolt.url.parser", new UrlParser, 4).shuffleGrouping("spout.twitter")
		builder.setBolt("bolt.url.counter", new Counter(10), 1).shuffleGrouping("bolt.url.parser")
		builder.setBolt("bolt.word.parser", new WordParser, 4).shuffleGrouping("spout.twitter")
		builder.setBolt("bolt.word.filter", new WordFilter(Array("a", "the", "not")), 4).shuffleGrouping("bolt.word.parser")
		builder.setBolt("bolt.word.counter", new Counter(10), 1).shuffleGrouping("bolt.word.filter")

		val conf = new Config
		conf.setMaxSpoutPending(1)
		val cluster = new LocalCluster
		cluster.submitTopology("test", conf, builder.createTopology)
		Thread.sleep(60000)
		cluster.deactivate("test")
		Thread.sleep(5000)
		cluster.shutdown
	}
}