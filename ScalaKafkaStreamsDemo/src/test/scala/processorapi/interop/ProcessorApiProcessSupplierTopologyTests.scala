package processorapi.interop

import java.io._
import java.util.Properties

import common.PropsHelper
import entities.Contributor
import org.apache.kafka.common.serialization.{Serdes, _}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest._
import serialization.{JSONDeserializer, JSONSerde}

import scala.io.Source


class ProcessorApiProcessSupplierTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = PropsHelper.createBasicStreamProperties("processor-api-process-supplier-application", "localhost:9092")
  val stringDeserializer: StringDeserializer = new StringDeserializer
  val contributorDeserializer: JSONDeserializer[Contributor] = new JSONDeserializer[Contributor]

  before {
  }

  after {
  }


  test("Should produce correct output") {

    //arrange

    val path ="c:\\temp\\kafka-streams-process-supplier.txt"
    new File(path).delete()
    val pw = new PrintWriter(new File(path))

    val recordFactory: ConsumerRecordFactory[java.lang.String, Array[Byte]] =
        new ConsumerRecordFactory[java.lang.String, Array[Byte]](new StringSerializer, Serdes.ByteArray().serializer())
    val processorApiProcessSupplierTopology = new ProcessorApiProcessSupplierTopology(pw)
    val jsonSerde = new JSONSerde[Contributor]

    //NOTE : You may find you need to play with these Config values in order
    //to get the stateful operation to work correctly/how you want it to
    //    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    //    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760.asInstanceOf[Object])
    //    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    //By playing around with these values you should be able to find the values that work for you
    //WARNING : Chaning these settings may have impact on the tests, as less frequent commits/state store
    //cache flushing may occur
    val testDriver = new TopologyTestDriver(processorApiProcessSupplierTopology.createTopolgy(), props)

    var a = 0;

    // for loop execution with a range
    for( a <- 0 to 4) {

      val contributor = Contributor("sacha@here.com", 0f, System.currentTimeMillis)
      val contributorBytes = jsonSerde.serializer().serialize("", contributor)

      //Use the custom JSONSerde[Contributor]
      testDriver.pipeInput(recordFactory.create("ProcessorApiProcessorSupplierInputTopic", contributor.email, contributorBytes, 9995L))
    }

    val lines = Source.fromFile(path).getLines.toList

    assert(lines.length == 5)
    assert(lines(0) == "key sacha@here.com has been seen 1 times")
    assert(lines(1) == "key sacha@here.com has been seen 2 times")
    assert(lines(2) == "key sacha@here.com has been seen 3 times")
    assert(lines(3) == "key sacha@here.com has been seen 4 times")
    assert(lines(4) == "key sacha@here.com has been seen 5 times")

    processorApiProcessSupplierTopology.stop()
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