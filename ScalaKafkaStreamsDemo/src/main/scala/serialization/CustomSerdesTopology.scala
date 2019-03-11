package serialization

import java.time.Duration
import java.util.Properties

import common.PropsHelper
import entities.Rating
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}


class CustomSerdesTopology extends App {

//


  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "custom-serdes-application", "localhost:9092")

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

    val stringSerde = Serdes.String
    val ratingSerde = new JSONSerde[Rating]
    val listRatingSerde = new JSONSerde[List[Rating]]
    val builder: StreamsBuilder = new StreamsBuilder

    val ratings: KStream[String, Rating] =
          builder.stream[String, Rating]("CustomSerdesInputTopic")
            (Consumed.with(stringSerde, ratingSerde))

    //When aggregating a grouped stream, you must provide an initializer (e.g., aggValue = 0)
    //and an “adder” aggregator (e.g., aggValue + curValue). When aggregating a grouped table,
    //you must provide a “subtractor” aggregator (think: aggValue - oldValue).
    val groupedBy = ratings.groupByKey
    val aggregatedTable =
      groupedBy
        .aggregate[List[Rating]](List[Rating]())((aggKey, newValue, aggValue) => newValue :: aggValue)
        (Materialized.with(stringSerde, listRatingSerde))

    aggregatedTable.toStream.to("CustomSerdesOutputTopic")
      (Produced.with(stringSerde, listRatingSerde))

    builder.build()
  }
}
