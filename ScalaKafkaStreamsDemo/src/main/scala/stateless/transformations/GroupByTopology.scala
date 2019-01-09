package stateless.transformations

import java.time.Duration
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.streams.kstream.Serialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}


/**
  * This example simply maps values from 'InputTopic' to 'OutputTopic'
  * with no changes
  */
class GroupByTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "stateless-groupBy-application", "localhost:9092")

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
    val textLines: KStream[Int, String] =
      builder.stream[Int, String]("InputTopic")


    //Groups the records by a new key, which may be of a different key type. When grouping a table, you may also specify
    //a new value and value type. groupBy is a shorthand for selectKey(...).groupByKey().
    //Grouping is a prerequisite for aggregating a stream or a table and ensures that data is properly partitioned (“keyed”)
    //for subsequent operations.
    //When to set explicit SerDes: Variants of groupBy exist to override the configured default SerDes of your application,
    //which you must do if the key and/or value types of the resulting KGroupedStream or KGroupedTable do not match the
    //configured default SerDes.
    //
    //Note
    //
    //Grouping vs. Windowing: A related operation is windowing, which lets you control how to “sub-group” the grouped records
    //of the same key into so-called windows for stateful operations such as windowed aggregations or windowed joins.
    //Always causes data re-partitioning: groupBy always causes data re-partitioning. If possible use groupByKey instead,
    //which will re-partition data only if required.
    textLines.flatMapValues(x => x.toLowerCase.split("\\W+"))
      .groupBy((key, word) => word)
      .count()
      .mapValues(x => x.toString)
      .toStream
      .to("groupedByOutputTopic")

    builder.build()
  }
}


