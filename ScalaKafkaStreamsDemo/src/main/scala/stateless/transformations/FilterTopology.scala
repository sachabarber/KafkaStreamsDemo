package stateless.transformations

import java.time.Duration
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}


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

    //Evaluates a boolean function for each element and retains those
    //for which the function returns true
    val above5 = textLines.filter((k,v) => v > 5)
    above5.to("Above5OutputTopic")

    //Evaluates a boolean function for each element and drops those
    //for which the function returns true.
    val belowOrEqualTo5 = textLines.filterNot((k,v) => v > 5)
    belowOrEqualTo5.to("BelowOrEqualTo5OutputTopic")

    builder.build()
  }
}
