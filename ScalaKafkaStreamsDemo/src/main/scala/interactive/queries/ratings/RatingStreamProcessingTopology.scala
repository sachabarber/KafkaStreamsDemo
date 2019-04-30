package serialization

import java.time.Duration
import java.util.Properties

import entities.Rating
import interactive.queries.ratings.RatingRestService
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.errors.BrokerNotFoundException
import org.apache.kafka.streams.kstream.{Aggregator, Initializer, Printed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.streams.{KafkaStreams, Topology}
import utils.{Retry, Settings, StateStores}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class RatingStreamProcessingTopology extends App {

  import Serdes._


  run(None, true)

  def run(topologyIn : Option[Topology], shouldClean : Boolean): Unit = {

    implicit val ec = ExecutionContext.global
    val props: Properties = Settings.createBasicStreamProperties(
      "rating-processing-application", "localhost:9092")


    val topology = topologyIn.getOrElse(createTopolgy())

    val restEndpoint: HostInfo = new HostInfo(Settings.restApiDefaultHostName, Settings.restApiDefaultPort)
    System.out.println(s"Connecting to Kafka cluster via bootstrap servers ${Settings.bootStrapServers}")
    System.out.println(s"REST endpoint at http://${restEndpoint.host}:${restEndpoint.port}")

    val maybeStreams =
      Retry.whileSeeingExpectedException[KafkaStreams,BrokerNotFoundException](10.seconds)(createStreams(topology, shouldClean, props))

    maybeStreams match {
      case Some(streams) => {
        val restService = new RatingRestService(streams, restEndpoint)
        restService.start()

        Runtime.getRuntime.addShutdownHook(new Thread(() => {
          streams.close()
          restService.stop
        }))
      }
      case None => {
        println("Quiting due to no streams available/unknown expcetion")
      }
    }

    //return unit
    ()
  }


  private def createStreams(topologyIn:Topology, shouldClean : Boolean, props:Properties) : KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(topologyIn,props)

    // Always (and unconditionally) clean local state prior to starting the processing topology.
    // We opt for this unconditional call here because this will make it easier for you to
    // play around with the example when resetting the application for doing a re-run
    // (via the Application Reset Tool,
    // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
    //
    // The drawback of cleaning up local state prior is that your app must rebuilt its local
    // state from scratch, which will take time and will require reading all the state-relevant
    // data from the Kafka cluster over the network.
    // Thus in a production scenario you typically do not want to clean up always as we do
    // here but rather only when it is truly needed, i.e., only under certain conditions
    // (e.g., the presence of a command line flag for your app).
    // See `ApplicationResetExample.java` for a production-like example.

    val t = Try {

      if(shouldClean) {
        streams.cleanUp()
      }
      streams.start()
      streams
    }
     t match {
       case Success(x) => x
       case Failure(e) => {
         null
       }
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
      builder.stream[String, Rating]("RatingInputTopic")

    import org.apache.kafka.streams.state.Stores
    val ratingByEmailStoreName = StateStores.RATINGS_BY_EMAIL_STORE
    val ratingByEmailStoreSupplied = Stores.inMemoryKeyValueStore(ratingByEmailStoreName)

    //When aggregating a grouped stream, you must provide an initializer (e.g., aggValue = 0)
    //and an “adder” aggregator (e.g., aggValue + curValue). When aggregating a grouped table,
    //you must provide a “subtractor” aggregator (think: aggValue - oldValue).
    val groupedBy = ratings.groupByKey

    //aggrgate by (user email -> their ratings)
    val ratingTable : KTable[String, List[Rating]] = groupedBy
        .aggregate[List[Rating]](List[Rating]())((aggKey, newValue, aggValue) => {
            newValue :: aggValue
      })(Materialized.as(ratingByEmailStoreSupplied))

    ratingTable.mapValues((k,v) => {
      val theKey = k
      val theValue = v
      v
    })


    builder.build()
  }
}
