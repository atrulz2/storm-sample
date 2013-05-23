import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import twitter4j.conf.ConfigurationBuilder
import scala.collection.mutable.Queue
import java.util.concurrent.LinkedBlockingQueue
import twitter4j.Status
import twitter4j.StatusListener
import twitter4j.StatusDeletionNotice
import twitter4j.TwitterStreamFactory
import twitter4j.StallWarning
import backtype.storm.tuple.Values
import backtype.storm.tuple.Fields
import twitter4j.TwitterStream
import twitter4j.FilterQuery

class TwitterStreamingSpout(keywords: List[String]) extends BaseRichSpout {
	private var collector: SpoutOutputCollector = null
	private var queue: LinkedBlockingQueue[Status] = null
	private var stream: TwitterStream = null

	override def open(conf: java.util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector) = {
		this.collector = collector
		this.queue = new LinkedBlockingQueue[Status](10000)
		// initialize the twitter stream
		val config = new ConfigurationBuilder()
			.setOAuthConsumerKey("Vxv9jb0n7YHeZKMjsbWdw")
			.setOAuthConsumerSecret("VT4KUZUXFVkXcv9qgfO0Do2QtTr6mgpb1eBH6QxMXU")
			.setOAuthAccessToken("1401345380-57lyPJDqWAaemzNscw6WIXwF3t9FZtcsQlboq3z")
			.setOAuthAccessTokenSecret("tcSPbOEI4Cp4TmuS8xNtP7IRq2oMCQLR37QFVjZrhUU")
			.build
		val statusListener = new StatusListener() {
			override def onStatus(status: Status) = queue.offer(status)
			override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) = ()
			override def onTrackLimitationNotice(numberOfLimitedStatuses: Int)  = ()
			override def onException(ex: Exception) = ()
			override def onScrubGeo(arg0: Long, arg1: Long) = ()
			override def onStallWarning(warning: StallWarning)  = ()
		};
		stream = new TwitterStreamFactory(config).getInstance
		stream.addListener(statusListener)
		stream.filter(new FilterQuery().track(keywords.toArray))
	}

	override def declareOutputFields(declarer: OutputFieldsDeclarer) = declarer.declare(new Fields("tweet"))

	override def nextTuple() = {
		val tweet = queue.poll
		if (tweet != null)
			collector.emit(new Values(tweet))
		else
			Thread.sleep(10)	
	}
	
	override def close() = {
		stream.cleanUp
		stream.shutdown
	}

}