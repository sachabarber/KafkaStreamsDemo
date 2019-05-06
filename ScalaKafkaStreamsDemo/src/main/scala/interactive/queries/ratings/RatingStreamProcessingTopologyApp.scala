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
import java.util.concurrent.CountDownLatch

object RatingStreamProcessingTopologyApp extends App {

  import Serdes._


  implicit val ec = ExecutionContext.global
  val doneSignal = new CountDownLatch(1)

  run()

  private def run(): Unit = {

    val restEndpoint: HostInfo = new HostInfo(Settings.restApiDefaultHostName, Settings.restApiDefaultPort)
    System.out.println(s"Connecting to Kafka cluster via bootstrap servers ${Settings.bootStrapServers}")
    System.out.println(s"REST endpoint at http://${restEndpoint.host}:${restEndpoint.port}")

    val props: Properties = Settings.createRatingStreamsProperties()
    val topology = new RatingStreamProcessingTopology().createTopolgy()
    val streams: KafkaStreams = new KafkaStreams(topology,props)

    val restService = new RatingRestService(streams, restEndpoint)

    //Can only add this in State == CREATED
    streams.setUncaughtExceptionHandler(( thread :Thread, throwable : Throwable) => {
      println(s"============> ${throwable.getMessage}")
      shutDown(streams,restService)

    })

    streams.setStateListener((newState, oldState) => {
      if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
        restService.setReady(true)
      } else if (newState != KafkaStreams.State.RUNNING) {
        restService.setReady(false)
      }
    })

    restService.start()

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      shutDown(streams,restService)
    }))

    println("Starting KafkaStream")

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
    streams.cleanUp
    streams.start

    doneSignal.await
   ()
  }


  private def shutDown(streams: KafkaStreams, restService: RatingRestService): Unit = {
    doneSignal.countDown
    streams.close()
    restService.stop
  }
}

