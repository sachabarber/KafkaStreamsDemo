package stateful.transformations.aggregating

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{Materialized, _}
import org.apache.kafka.streams.{KafkaStreams, Topology}
import utils.Settings


class ReduceTopology extends App {

  import Serdes._

  val props: Properties = Settings.createBasicStreamProperties(
    "stateless-reduce-application", "localhost:9092")

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
      builder.stream[String, String]("ReduceInputTopic")


    //lets create a named reduceStore state store
    //The "persistentKeyValueStore" is one of the pre-canned state store types
    //and as such logging is enabled, so the ChangeLog topic to persist state
    import org.apache.kafka.streams.state.Stores
    val reduceStoreName = "reduceStore"
    val reduceStoreSupplied = Stores.persistentKeyValueStore(reduceStoreName)

    //by using this dummy topic we can go straight from a KSTream to kTable
    textLines.to("Dummy-ReduceInputTopic")
    val userProfiles : KTable[String, String] = builder.table("Dummy-ReduceInputTopic")


    val groupedTable = userProfiles.groupBy((user, region) => (region, user.length()))
    val reduced: KTable[String, Integer] =
      groupedTable.reduce(
        (aggValue : Int, newValue: Int) => aggValue + newValue,
        (aggValue: Int, oldValue: Int) => aggValue - oldValue)
      .mapValues(v => new Integer(v))

    val finalStream = reduced.toStream
    finalStream.to("ReduceOutputTopic")

    builder.build()
  }
}


