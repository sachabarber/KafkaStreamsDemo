package processorapi.interop

import java.time.Duration
import java.util
import java.util.Properties

import common.PropsHelper
import entities.Contributor
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.scala.{StreamsBuilder, kstream}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}
import serialization.JSONSerde


class ProcessorApiTransformValuesTopology extends App {

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "processor-api-transform-values-application", "localhost:9092")

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
    implicit val contributorSerde = new JSONSerde[Contributor]
    implicit val consumed = kstream.Consumed.`with`(stringSerde, contributorSerde)
    implicit val materializer = Materialized.`with`(stringSerde, contributorSerde)

    import org.apache.kafka.streams.state.Stores
    val contributorStoreName = "contributorStore"

    val logConfig = new util.HashMap[String, String]
    logConfig.put("retention.ms", "172800000")
    logConfig.put("retention.bytes", "10000000000")
    logConfig.put("cleanup.policy", "compact,delete")
    val contributorStoreSupplier = Stores.inMemoryKeyValueStore(contributorStoreName)
    val contributorStoreBuilder = Stores.keyValueStoreBuilder(contributorStoreSupplier, Serdes.String, contributorSerde)
      .withLoggingEnabled(logConfig)
      .withCachingEnabled()


    val builder: StreamsBuilder = new StreamsBuilder
    val contribs: KStream[String, Contributor] =
          builder.stream[String, Contributor]("ProcessorApiTransformValuesInputTopic")

    builder.addStateStore(contributorStoreBuilder)

    contribs
      .transformValues(new ContributorTranformSupplier, contributorStoreName)
      .to("ProcessorApiTransformValuesOutputTopic")(Produced.`with`(stringSerde, contributorSerde))

    builder.build()
  }
}
