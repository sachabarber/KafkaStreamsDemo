package stateless.transformations

import java.time.Duration
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}


class MapTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "stateless-map-application", "localhost:9092")

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
    val textLines: KStream[Int, Int] =
      builder.stream[Int, Int]("InputTopic")

    //Takes one record and produces one record. You can modify the record key and value, including their types.
    //Marks the stream for data re-partitioning: Applying a grouping or a join after map will result in re-partitioning
    //of the records. If possible use mapValues instead, which will not cause data re-partitioning.
    val mapped = textLines.map((k,v) => {
        (k + 1, v + 2)
    })
    mapped.to("mappedOutputTopic")


    //Takes one record and produces one record, while retaining the key of the original record.
    //You can modify the record value and the value type. (KStream details, KTable details)
    //mapValues is preferable to map because it will not cause data re-partitioning. However, it
    //does not allow you to modify the key or key type like map does.
    val mappedValues = textLines.mapValues((k,v) => {
        v + 10
    })
    mappedValues.to("mappedValuesOutputTopic")

    builder.build()
  }
}


