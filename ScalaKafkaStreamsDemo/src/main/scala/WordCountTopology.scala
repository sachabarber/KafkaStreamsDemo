import java.time.Duration
import java.util
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._


/**
  * This example simply maps values from 'InputTopic' to 'OutputTopic'
  * with no changes
  */
class WordCountTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
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
    val wordCounts = textLines.flatMapValues(x=> x.toLowerCase.split("\\W+"))
                    .groupBy((key, word) => word)
                    .count()(Materialized.as(wordCountStoreSupplied))


    wordCounts.toStream.to("WordsWithCountsTopic")
    builder.build()
  }

  def wordCountToplogyWithConfiguredStore() : Topology = {

    import org.apache.kafka.streams.state.Stores
    val wordCountStoreName = "wordCountStore"

    val logConfig = new util.HashMap[String, String]
    val wordCountStoreSupplier = Stores.inMemoryKeyValueStore(wordCountStoreName)
    val wordCountStoreBuilder = Stores.keyValueStoreBuilder(wordCountStoreSupplier, Serdes.String, Serdes.Long)
      .withLoggingEnabled(logConfig)
      .withCachingEnabled()


    val builder = new StreamsBuilder()

    builder.addStateStore(wordCountStoreBuilder)

    val textLines: KStream[String, String] = builder.stream("TextLinesTopic")
    val wordCounts = textLines.flatMapValues(x=> x.toLowerCase.split("\\W+"))
      .groupBy((key, word) => word)
      .count()(Materialized.as(wordCountStoreName))


    wordCounts.toStream.to("WordsWithCountsTopic")
    builder.build()
  }

}
