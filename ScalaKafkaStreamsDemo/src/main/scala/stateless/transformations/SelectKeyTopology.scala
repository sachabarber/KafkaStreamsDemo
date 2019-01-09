package stateless.transformations

import java.time.Duration
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}


/**
  * This example simply maps values from 'InputTopic' to 'OutputTopic'
  * with no changes
  */
class SelectKeyTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "stateless-selectKey-application", "localhost:9092")

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

    // Assigns a new key – possibly of a new key type – to each record.
    // Calling selectKey(mapper) is the same as calling map((key, value) -> mapper(key, value), value).
    // Marks the stream for data re-partitioning: Applying a grouping or a join after selectKey will
    // result in re-partitioning of the records.
    val mapped = textLines.selectKey((k,v) => {
        k.toString
    })
    mapped.to("selectKeyOutputTopic")

    builder.build()
  }
}


