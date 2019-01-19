package stateless.transformations

import java.io._
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._

class FlatMapTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = PropsHelper.createBasicStreamProperties("stateless-flatMap-application", "localhost:9092")
  val integerDeserializer: IntegerDeserializer = new IntegerDeserializer

  before {
  }

  after {
  }


  test("Should produce correct output") {

    //arrange
    val recordFactory: ConsumerRecordFactory[java.lang.Integer, java.lang.Integer] =
      new ConsumerRecordFactory[java.lang.Integer, java.lang.Integer](new IntegerSerializer, new IntegerSerializer)
    val flatMapTopology = new FlatMapTopology()
      val testDriver = new TopologyTestDriver(flatMapTopology.createTopolgy(), props)

    //act
    testDriver.pipeInput(recordFactory.create("InputTopic", 1, 2, 9995L))

    //assert

    // flatMap is yielding this
    //    List(
    //      (k + 1, v + 2),
    //      (k + 3, v + 4)
    //    )
    OutputVerifier.compareValue(testDriver.readOutput("flatMappedOutputTopic", integerDeserializer, integerDeserializer),
      4.asInstanceOf[Integer])
    OutputVerifier.compareValue(testDriver.readOutput("flatMappedOutputTopic", integerDeserializer, integerDeserializer),
      6.asInstanceOf[Integer])
    val result1 = testDriver.readOutput("flatMappedOutputTopic", integerDeserializer, integerDeserializer)
    assert(result1 == null)

    // flatMapValues is yielding this
    //    List(
    //      v + 10,
    //      v + 20
    //    )
    OutputVerifier.compareValue(testDriver.readOutput("flatMappedValuesOutputTopic", integerDeserializer, integerDeserializer),
      12.asInstanceOf[Integer])
    OutputVerifier.compareValue(testDriver.readOutput("flatMappedValuesOutputTopic", integerDeserializer, integerDeserializer),
      22.asInstanceOf[Integer])
    val result2 = testDriver.readOutput("flatMappedValuesOutputTopic", integerDeserializer, integerDeserializer)
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