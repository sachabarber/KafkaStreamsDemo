package stateless.transformations

import java.time.Duration
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, KeyValue, Topology}


/**
  * This example simply maps values from 'InputTopic' to 'OutputTopic'
  * with no changes
  */
class FlatMapTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "stateless-flatMap-application", "localhost:9092")

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

    //Takes one record and produces zero, one, or more records.
    //You can modify the record keys and values, including their types
    val flatMapped = textLines.flatMap((k,v) => {
      List(
        (k + 1, v + 2),
        (k + 3, v + 4)
      )
    })
    flatMapped.to("flatMappedOutputTopic")


    //Takes one record and produces zero, one, or more records, while retaining the key of the original record.
    //You can modify the record values and the value type.
    //flatMapValues is preferable to flatMap because it will not cause data re-partitioning.
    //However, you cannot modify the key or key type like flatMap does
    val flatMappedValues = textLines.flatMapValues((k,v) => {
      List(
        v + 10,
        v + 20
      )
    })
    flatMappedValues.to("flatMappedValuesOutputTopic")

    builder.build()
  }
}


