package serialization

import java.time.Duration
import java.util.Properties

import common.PropsHelper
import entities.Rating
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}


class CustomSerdesTopology extends App {

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

    implicit val stringSerde = Serdes.String
    implicit val ratingSerde = new JSONSerde[Rating]
    implicit val listRatingSerde = new JSONSerde[List[Rating]]
    implicit val consumed = kstream.Consumed.`with`(stringSerde, ratingSerde)
    implicit val materializer = Materialized.`with`(stringSerde, listRatingSerde)
    implicit val grouped = Grouped.`with`(stringSerde, ratingSerde)

    val builder: StreamsBuilder = new StreamsBuilder
    val ratings: KStream[String, Rating] =
          builder.stream[String, Rating]("CustomSerdesInputTopic")

    //When aggregating a grouped stream, you must provide an initializer (e.g., aggValue = 0)
    //and an “adder” aggregator (e.g., aggValue + curValue). When aggregating a grouped table,
    //you must provide a “subtractor” aggregator (think: aggValue - oldValue).
    val groupedBy = ratings.groupByKey
    val aggregatedTable =
      groupedBy
        .aggregate[List[Rating]](List[Rating]())((aggKey, newValue, aggValue) => newValue :: aggValue)

    var finalStream = aggregatedTable.toStream
    finalStream.peek((key, values) => {

      val theKey = key
      val theValues = values

    })


    finalStream.to("CustomSerdesOutputTopic")(Produced.`with`(stringSerde, listRatingSerde))

    builder.build()
  }
}
