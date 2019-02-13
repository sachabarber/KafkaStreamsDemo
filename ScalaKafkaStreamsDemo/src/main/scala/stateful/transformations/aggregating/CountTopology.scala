package stateful.transformations.aggregating

import java.time.Duration
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{Materialized, _}
import org.apache.kafka.streams.{KafkaStreams, Topology}


class CountTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "stateless-count-application", "localhost:9092")

  run()

  private def run(): Unit = {
    val topology = createTopolgy()
    val streams: KafkaStreams = new KafkaStreams(topology, props)
    streams.start()
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }

  def createTopolgy(): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder
    val textLines: KStream[String, String] =
      builder.stream[String, String]("CountInputTopic")


    //lets create a named wordCountStore state store
    //The "persistentKeyValueStore" is one of the pre-canned state store types
    //and as such logging is enabled, so the ChangeLog topic to persist state
    import org.apache.kafka.streams.state.Stores
    val wordCountStoreName = "wordCountStore"
    val wordCountStoreSupplied = Stores.persistentKeyValueStore(wordCountStoreName)

    val wordCounts = textLines
      .flatMapValues(x => x.toLowerCase.split("\\W+"))
      .groupBy((key, word) => word)
      .count()(Materialized.as(wordCountStoreSupplied))
    wordCounts
      .toStream
      .peek((k,v) =>
      {
        val theKey = k
        val theValue =v
      })
      .to("WordsWithCountsOutputTopic")

    builder.build()
  }
}


