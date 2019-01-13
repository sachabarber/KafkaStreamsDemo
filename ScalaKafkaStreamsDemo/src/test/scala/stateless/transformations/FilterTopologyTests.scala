package stateless.transformations

import java.io._
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._
import scala.collection.JavaConverters._

class FilterTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = PropsHelper.createBasicStreamProperties("stateless-filter-application", "localhost:9092")
  val stringDeserializer: StringDeserializer = new StringDeserializer
  val longDeserializer: LongDeserializer = new LongDeserializer

  before {
  }

  after {
  }


  test("Should produce correct output") {

    //arrange
    val recordFactory: ConsumerRecordFactory[String, java.lang.Long] =
      new ConsumerRecordFactory[String, java.lang.Long](new StringSerializer, new LongSerializer)
    val filterTopology = new FilterTopology()
    val testDriver = new TopologyTestDriver(filterTopology.createTopolgy(), props)

    //act
    List(0L,6L,7L).foreach(x => {
      testDriver.pipeInput(recordFactory.create("InputTopic", "key", java.lang.Long.valueOf(x), 9995L + 1))
    })

    //assert
    OutputVerifier.compareValue(testDriver.readOutput("Above5OutputTopic", stringDeserializer, longDeserializer), 6L.asInstanceOf[java.lang.Long])
    OutputVerifier.compareValue(testDriver.readOutput("Above5OutputTopic", stringDeserializer, longDeserializer), 7L.asInstanceOf[java.lang.Long])
    val result = testDriver.readOutput("OddTopic", stringDeserializer, longDeserializer)
    assert(result == null)

    OutputVerifier.compareValue(testDriver.readOutput("BelowOrEqualTo5OutputTopic", stringDeserializer, longDeserializer), 0L.asInstanceOf[java.lang.Long])
    val result2 = testDriver.readOutput("BelowOrEqualTo5OutputTopic", stringDeserializer, longDeserializer)
    assert(result2 == null)

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