package stateless.transformations

import java.io.PrintWriter
import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}
import utils.Settings


class PeekTopology(val pw: PrintWriter) extends App {

  import Serdes._

  val props: Properties = Settings.createBasicStreamProperties(
    "stateless-peek-application", "localhost:9092")

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

    // Performs a stateless action on each record, and returns an unchanged stream.
    //
    // You would use peek to cause side effects based on the input data (similar to foreach)
    // and continue processing the input data (unlike foreach, which is a terminal operation).
    // Peek returns the input stream as-is; if you need to modify the input stream, use map or mapValues instead.
    // peek is helpful for use cases such as logging or tracking metrics or for debugging and troubleshooting.
    //
    // Note on processing guarantees: Any side effects of an action (such as writing to external systems)
    // are not trackable by Kafka, which means they will typically not benefit from Kafka’s processing guarantees.
    val peeked = textLines.peek((k,v)=> {
      pw.write(s"Saw input value line '$v'\r\n")
      pw.flush()
    })
    peeked.to("peekedOutputTopic")


    builder.build()
  }
}


