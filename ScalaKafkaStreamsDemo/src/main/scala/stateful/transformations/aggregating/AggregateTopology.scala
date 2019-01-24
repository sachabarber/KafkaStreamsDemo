package stateful.transformations.aggregating

import java.time.Duration
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{Materialized, _}
import org.apache.kafka.streams.{KafkaStreams, Topology}


/**
  * This example simply maps values from 'InputTopic' to 'OutputTopic'
  * with no changes
  */
class AggregateTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "stateless-aggregate-application", "localhost:9092")

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
      builder.stream[Int, Int]("AggregateKeyInputTopic")

    implicit val matererlized: Materialized[Int, Long, ByteArrayKeyValueStore]
      = Materialized.as("aggregated-stream-store")

    //Rolling aggregation. Aggregates the values of (non-windowed) records by the grouped key.
    //Aggregating is a generalization of reduce and allows, for example, the aggregate value to
    //have a different type than the input values. (KGroupedStream details, KGroupedTable details)
    //
    //When aggregating a grouped stream, you must provide an initializer (e.g., aggValue = 0)
    //and an “adder” aggregator (e.g., aggValue + curValue). When aggregating a grouped table,
    //you must provide a “subtractor” aggregator (think: aggValue - oldValue).
    val groupedBy = textLines.groupByKey
    val aggregatedTable =
      groupedBy
        .aggregate[Long](0L)((aggKey, newValue, aggValue) => aggValue + newValue)(matererlized)
    aggregatedTable
        .toStream
          .peek((k,v) =>
          {
            val theKey = k
            val theValue =v
          })
        .to("aggregateOutputTopic")
    builder.build()
  }
}


