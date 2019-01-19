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
    val groupByKeyTextLines: KStream[Int, String] =
      builder.stream[Int, String]("GroupByKeyInputTopic")
    val groupByTextLines: KStream[Int, String] =
      builder.stream[Int, String]("GroupByInputTopic")


    //Groups the records by the existing key. (details)
    //
    //Grouping is a prerequisite for aggregating a stream or a table and ensures that data is
    //properly partitioned (“keyed”) for subsequent operations.
    //
    //When to set explicit SerDes: Variants of groupByKey exist to override the configured default
    //SerDes of your application, which you must do if the key and/or value types of the resulting
    //KGroupedStream do not match the configured default SerDes.

    //Note
    //
    //Grouping vs. Windowing: A related operation is windowing, which lets you control how to “sub-group”
    //the grouped records of the same key into so-called windows for stateful operations such as windowed
    //aggregations or windowed joins.
    //Causes data re-partitioning if and only if the stream was marked for re-partitioning. groupByKey is
    //preferable to groupBy because it re-partitions data only if the stream was already marked for
    //re-partitioning. However, groupByKey does not allow you to modify the key or key type like groupBy does.


    //NOTE : You may find you need to play with these Config values in order
    //to get the stateful operation to work correctly/how you want it to
    //    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    //    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760.asInstanceOf[Object])
    //    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    //By playing around with these values you should be able to find the values that work for you
    //WARNING : Chaning these settings may have impact on the tests, as less frequent commits/state store
    //cache flushing may occur
    groupByKeyTextLines.groupByKey
        .count()
        .mapValues(x => x.toString)
        .toStream
        .to("groupedByKeyOutputTopic")




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

    //NOTE : You may find you need to play with these Config values in order
    //to get the stateful operation to work correctly/how you want it to
    //    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    //    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760.asInstanceOf[Object])
    //    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    //By playing around with these values you should be able to find the values that work for you
    //WARNING : Chaning these settings may have impact on the tests, as less frequent commits/state store
    //cache flushing may occur
    groupByTextLines.flatMapValues(x => x.toLowerCase.split("\\W+"))
      .groupBy((key, word) => word)
      .count()
      .mapValues(x => x.toString)
      .toStream
      .to("groupedByOutputTopic")

    builder.build()
  }
}


