package windowing

import java.time.Duration
import java.util
import java.util.Properties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}
import utils.Settings


class TumblingWordCountTopology extends App {

  import Serdes._

  val props: Properties = Settings.createBasicStreamProperties(
    "tumbling-window-wordcount-application","localhost:9092")

  run()

  private def run(): Unit = {
    val topology = wordCountToplogy()
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
    val textLines: KStream[String, String] = builder.stream[String, String]("TextLinesTopic")
    val wordCounts = textLines.flatMapValues(x => x.toLowerCase.split("\\W+"))
                    .groupBy((key, word) => word)
      .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
      .count()
    wordCounts.toStream.to("WordsWithCountsTopic")
    builder.build()
  }
}
