package stateless.transformations

import java.io._
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.common.serialization.{StringDeserializer, _}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._

class SelectKeyTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = PropsHelper.createBasicStreamProperties("stateless-selectKey-application", "localhost:9092")
  val integerDeserializer: IntegerDeserializer = new IntegerDeserializer
  val stringDeserializer: StringDeserializer = new StringDeserializer

  before {
  }

  after {
  }


  test("Should produce correct output") {

    //arrange
    val recordFactory: ConsumerRecordFactory[java.lang.Integer, java.lang.Integer] =
      new ConsumerRecordFactory[java.lang.Integer, java.lang.Integer](new IntegerSerializer, new IntegerSerializer)
    val selectKeyTopology = new SelectKeyTopology()
    val testDriver = new TopologyTestDriver(selectKeyTopology.createTopolgy(), props)

    //act
    testDriver.pipeInput(recordFactory.create("InputTopic", 1, 2, 9995L))

    //assert

    // selectKey is yielding this
    //      ("1", 2)
    //    )
    OutputVerifier.compareKeyValue(testDriver.readOutput("selectKeyOutputTopic", stringDeserializer, integerDeserializer),
      "1",2.asInstanceOf[Integer])
    val result1 = testDriver.readOutput("selectKeyOutputTopic", stringDeserializer, integerDeserializer)
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