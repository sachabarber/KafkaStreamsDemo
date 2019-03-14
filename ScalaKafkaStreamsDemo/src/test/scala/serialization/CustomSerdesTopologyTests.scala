package serialization

import java.io._
import java.lang
import java.util.Properties

import common.PropsHelper
import entities.Rating
import org.apache.kafka.common.serialization.{LongDeserializer, _}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._
import org.apache.kafka.common.serialization.Serdes


class CustomSerdesTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = PropsHelper.createBasicStreamProperties("custom-serdes-application", "localhost:9092")
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
    val customSerdesTopology = new CustomSerdesTopology()


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
    val testDriver = new TopologyTestDriver(customSerdesTopology.createTopolgy(), props)

    //Use the custom JSONSerde[Rating]
    testDriver.pipeInput(recordFactory.create("CustomSerdesInputTopic", rating.toEmail, ratingBytes, 9995L))

    val result = testDriver.readOutput("CustomSerdesOutputTopic", stringDeserializer, ratingLIstDeserializer)

    OutputVerifier.compareKeyValue(result, "sacha@here.com",List(Rating("jarden@here.com","sacha@here.com", 1.5f)))
    val result1 = testDriver.readOutput("CustomSerdesOutputTopic", stringDeserializer, ratingLIstDeserializer)
    assert(result1 == null)

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