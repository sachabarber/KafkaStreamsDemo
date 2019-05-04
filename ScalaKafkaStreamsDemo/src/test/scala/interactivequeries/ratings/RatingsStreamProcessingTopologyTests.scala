package interactivequeries.ratings

import java.io._
import java.util.Properties

import entities.Rating
import interactive.queries.ratings.RatingStreamProcessingTopology
import org.apache.kafka.common.serialization.{Serdes, _}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._
import serialization.{JSONDeserializer, JSONSerde}
import utils.{Settings, StateStores}

import scala.concurrent.{ExecutionContext, Future}


class RatingsStreamProcessingTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = Settings.createBasicStreamProperties("rating-processing-application", "localhost:9092")
  val stringDeserializer: StringDeserializer = new StringDeserializer
  val ratingLIstDeserializer: JSONDeserializer[List[Rating]] = new JSONDeserializer[List[Rating]]

  before {
  }

  after {
  }


  test("Should produce correct output") {

    //arrange
    val recordFactory: ConsumerRecordFactory[java.lang.String, Array[Byte]] =
        new ConsumerRecordFactory[java.lang.String, Array[Byte]](new StringSerializer, Serdes.ByteArray().serializer())
    val ratingStreamProcessingTopology = new RatingStreamProcessingTopology()


    val jsonSerde = new JSONSerde[Rating]

    val rating = Rating("jarden@here.com","sacha@here.com", 1.5f)
    val ratingBytes = jsonSerde.serializer().serialize("", rating)


    //NOTE : You may find you need to play with these Config values in order
    //to get the stateful operation to work correctly/how you want it to
    //    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    //    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760.asInstanceOf[Object])
    //    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    //By playing around with these values you should be able to find the values that work for you
    //WARNING : Chaning these settings may have impact on the tests, as less frequent commits/state store
    //cache flushing may occur
    val theTopology = ratingStreamProcessingTopology.createTopolgy()
    val testDriver = new TopologyTestDriver(theTopology, props)

    //Use the custom JSONSerde[Rating]
    testDriver.pipeInput(recordFactory.create("rating-submit-topic", rating.toEmail, ratingBytes, 9995L))

    val ratingsByEmailStore: KeyValueStore[String, List[Rating]] = testDriver.getKeyValueStore(StateStores.RATINGS_BY_EMAIL_STORE)
    val ratingStored = ratingsByEmailStore.get("sacha@here.com")

    cleanup(props, testDriver)
  }


  def cleanup(props:Properties, testDriver: TopologyTestDriver) = {

    try {
      //there is a bug on windows which causes this line to throw exception
      testDriver.close
    } catch {
      case e: Exception => {
        delete(new File("C:\\data\\kafka-streams"))
      }
    }
  }

  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
    file.delete
  }
}