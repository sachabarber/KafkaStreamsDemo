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
class FilterTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "stateless-filter-application", "localhost:9092")

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
    val textLines: KStream[String, Long] =
      builder.stream[String, Long]("InputTopic")
    val evens = textLines.filter((k,v) => v % 2 == 0)
    evens.to("OutputTopic")
    builder.build()
  }
}
