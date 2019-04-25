package stateless.transformations

import java.time.Duration
import java.util.Properties

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.StreamPartitioner
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}
import utils.Settings

class ThroughCustomPartitionerTopology() extends App {

  import Serdes._

  val props: Properties = Settings.createBasicStreamProperties(
    "stateless-custompartitioner-application", "localhost:9092")

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

    implicit val keySerde =  Serdes.Integer
    implicit val valueSerde =  Serdes.String

    val builder: StreamsBuilder = new StreamsBuilder
    implicit val consumed = kstream.Consumed.`with`(keySerde, valueSerde)

    val textLines: KStream[java.lang.Integer, String] =
      builder.stream[java.lang.Integer, String]("InputTopic")

    //should use implicit Produced with the through
    val repartitoned = textLines.through("InputTopic-RePartitioned")
      (Produced.`with`(new AlwaysOneCustomPartitioner()))

    val finalStream = repartitoned
      .mapValues((x) => s"${x} Through")

    finalStream.to("OutputTopic")
    builder.build()
  }
}

//Custom StreamPartitioner that ALWAYS just returns 1 for Partition
class AlwaysOneCustomPartitioner extends StreamPartitioner[java.lang.Integer, String] {
  override def partition(topic: String, key: java.lang.Integer, value: String, numPartitions: Int): Integer = {
    //just default it to 1
    1
  }
}