package stateless.transformations

import java.io.{File, PrintWriter}
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
class ForEachTopology(val pw: PrintWriter) extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "stateless-foreach-application", "localhost:9092")

  run()

  def stop() : Unit = {
    pw.close()
  }

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

    //Terminal operation. Performs a stateless action on each record.

    //You would use foreach to cause side effects based on the input data (similar to peek)
    //and then stop further processing of the input data (unlike peek, which is not a terminal operation).
    //Note on processing guarantees: Any side effects of an action (such as writing to
    //external systems) are not trackable by Kafka, which means they will typically not
    //benefit from Kafka’s processing guarantees.
    val flatMapped = textLines.foreach((k,v)=> {
      pw.write(s"Saw input value line '$v'\r\n")
      pw.flush()
    })

    builder.build()
  }
}


