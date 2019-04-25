package stateless.transformations

import java.io._
import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._
import utils.Settings

import scala.io.Source

class ThroughCustomPartitionerTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = Settings.createBasicStreamProperties("stateless-custompartitioner-application", "localhost:9092")
  val integerDeserializer: IntegerDeserializer = new IntegerDeserializer
  val stringDeserializer: StringDeserializer = new StringDeserializer

  before {
  }

  after {
  }


  test("Should produce correct output") {

    //arrange
    val recordFactory: ConsumerRecordFactory[java.lang.Integer, java.lang.String] =
      new ConsumerRecordFactory[java.lang.Integer, java.lang.String](new IntegerSerializer, new StringSerializer)
    val throughCustomPartitionerTopology = new ThroughCustomPartitionerTopology()
    val testDriver = new TopologyTestDriver(throughCustomPartitionerTopology.createTopolgy(), props)

    //act
    testDriver.pipeInput(recordFactory.create("InputTopic", 1, "1", 9995L))
    testDriver.pipeInput(recordFactory.create("InputTopic", 1, "2", 9995L + 1))

    //assert
    OutputVerifier.compareValue(testDriver.readOutput("OutputTopic", integerDeserializer, stringDeserializer),
     "1 Through")
    OutputVerifier.compareValue(testDriver.readOutput("OutputTopic", integerDeserializer, stringDeserializer),
      "2 Through")
    val result2 = testDriver.readOutput("OutputTopic", integerDeserializer, stringDeserializer)
    assert(result2 == null)

    cleanup(props, testDriver)
  }


  def cleanup(props: Properties, testDriver: TopologyTestDriver) = {

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