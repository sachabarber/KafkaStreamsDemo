package interactive.queries.ratings

import java.util.Properties

import entities.Rating
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.errors.BrokerNotFoundException
import org.apache.kafka.streams.state.{HostInfo, QueryableStoreTypes}
import org.apache.kafka.streams.KafkaStreams
import utils.{Retry, Settings, StateStores}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.util.Success

object RatingStreamProcessingTopologyApp extends App {

  import Serdes._


  implicit val ec = ExecutionContext.global

  run()

  private def run(): Unit = {

    val restEndpoint: HostInfo = new HostInfo(Settings.restApiDefaultHostName, Settings.restApiDefaultPort)
    System.out.println(s"Connecting to Kafka cluster via bootstrap servers ${Settings.bootStrapServers}")
    System.out.println(s"REST endpoint at http://${restEndpoint.host}:${restEndpoint.port}")

    val maybeStreams =
      Retry.whileSeeingExpectedException[KafkaStreams,BrokerNotFoundException](10.seconds)(createStreams)

    maybeStreams match {
      case Some(streams) => {

        while(!streams.state().isRunning)
        {
          System.out.println(s"IB Waiting for 'Running' state. state now is ${streams.state()}")
          Thread.sleep(1000)
        }
        System.out.println(s"After Waiting for 'Running' state. state now is ${streams.state()}")

        printStoreMetaData(streams)

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

  private def printStoreMetaData(streams:KafkaStreams) : Unit = {

    //KafkaStreams RUNNING is a re-entrant state, we could get it lost then appear again, so check here again
    while(!streams.state().isRunning)
    {
      System.out.println(s"IB Waiting for 'Running' state. state now is ${streams.state()}")
      Thread.sleep(1000)
    }
    System.out.println(s"After Waiting for 'Running' state. state now is ${streams.state()}")

    val md = streams.allMetadata()
    val mdStore = streams.allMetadataForStore(StateStores.RATINGS_BY_EMAIL_STORE)

    val maybeStore = StateStores.waitUntilStoreIsQueryableSync(
      StateStores.RATINGS_BY_EMAIL_STORE,
          QueryableStoreTypes.keyValueStore[String,List[Rating]](),
          streams)

    maybeStore match {
      case Some(store) => {
        val range = store.all
        val HASNEXT = range.hasNext
        import org.apache.kafka.streams.KeyValue
        while (range.hasNext      ) {
          val next = range.next
          System.out.print(s"key: ${next.key} value: ${next.value}")
        }
      }
      case None => {
        System.out.print(s"store not ready")
        throw new Exception("not ready")
      }
    }


  }


  private def createStreams() : KafkaStreams = {

    val props: Properties = Settings.createRatingStreamsProperties()
    val topology = new RatingStreamProcessingTopology().createTopolgy()

    val streams: KafkaStreams = new KafkaStreams(topology,props)

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

    //Can only add this in State == CREATED
    streams.setUncaughtExceptionHandler(( thread :Thread, throwable : Throwable) => {
      println(s"============> ${throwable.getMessage}")

    })

    //streams.cleanUp()
    streams.start()


    streams

  }
}

