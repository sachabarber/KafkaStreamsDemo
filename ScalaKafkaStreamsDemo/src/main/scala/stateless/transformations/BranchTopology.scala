package stateless.transformations

import java.time.Duration
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
class BranchTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "stateless-branch-application", "localhost:9092")

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
      builder.stream[String, String]("InputTopic")

    val predicates : List[(String, String) => Boolean] = List(
      (k,v) => v.startsWith("Odd"),
      (k,v) => v.startsWith("Even")
    )

    val branches : Array[KStream[String, String]] = textLines.branch(predicates:_*)
    branches(0).to("OddTopic")
    branches(1).to("EvenTopic")
    builder.build()
  }
}
