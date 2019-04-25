import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import utils.Settings


class WordCountTopology extends App {

  import Serdes._

  val props: Properties = Settings.createBasicStreamProperties(
    "straight-through-application","localhost:9092")

  run()

  private def run(): Unit = {
    val topology = wordCountToplogyWithConfiguredStore()
    val streams: KafkaStreams = new KafkaStreams(topology, props)
    streams.start()
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }

   def wordCountToplogy() : Topology = {

    import org.apache.kafka.streams.state.Stores
    val wordCountStoreName = "wordCountStore"
    val wordCountStoreSupplied = Stores.inMemoryKeyValueStore(wordCountStoreName)

    val builder = new StreamsBuilder()
    val textLines: KStream[String, String] = builder.stream("TextLinesTopic")
    val wordCounts = textLines.flatMapValues(x => x.toLowerCase.split("\\W+"))
                    .groupBy((key, word) => word)
                    .count()(Materialized.as(wordCountStoreSupplied))
    wordCounts.toStream.to("WordsWithCountsTopic")
    builder.build()
  }

  def wordCountToplogyWithConfiguredStore() : Topology = {

    import org.apache.kafka.streams.state.Stores
    val wordCountStoreName = "wordCountStore"

    val logConfig = new util.HashMap[String, String]
    logConfig.put("retention.ms", "172800000")
    logConfig.put("retention.bytes", "10000000000")
    logConfig.put("cleanup.policy", "compact,delete")
    val wordCountStoreSupplier = Stores.inMemoryKeyValueStore(wordCountStoreName)
    val wordCountStoreBuilder = Stores.keyValueStoreBuilder(wordCountStoreSupplier,
        Serdes.String, Serdes.Long)
      .withLoggingEnabled(logConfig)
      .withCachingEnabled()


    val builder = new StreamsBuilder()

    builder.addStateStore(wordCountStoreBuilder)

    //NOTE : You may find you need to play with these Config values in order
    //to get the stateful operation to work correctly/how you want it to
    //    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    //    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760.asInstanceOf[Object])
    //    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    //By playing around with these values you should be able to find the values that work for you
    //WARNING : Chaning these settings may have impact on the tests, as less frequent commits/state store
    //cache flushing may occur
    val textLines: KStream[String, String] = builder.stream("TextLinesTopic")
    val wordCounts = textLines.flatMapValues(x => x.toLowerCase.split("\\W+"))
      .groupBy((key, word) => word)
      .count()(Materialized.as(wordCountStoreName))


    wordCounts.toStream.to("WordsWithCountsTopic")
    builder.build()
  }

}
