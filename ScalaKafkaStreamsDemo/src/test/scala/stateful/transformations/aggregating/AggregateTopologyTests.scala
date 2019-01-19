package stateful.transformations.aggregating

import java.io._
import java.lang
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.common.serialization.{LongDeserializer, _}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._

class AggregateTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = PropsHelper.createBasicStreamProperties("stateless-aggregate-application", "localhost:9092")
  val integerDeserializer: IntegerDeserializer = new IntegerDeserializer
  val longDeserializer: LongDeserializer = new LongDeserializer

  before {
  }

  after {
  }


  test("Should produce correct output") {

    //arrange
    val recordFactory: ConsumerRecordFactory[java.lang.Integer, java.lang.Integer] =
      new ConsumerRecordFactory[java.lang.Integer, java.lang.Integer](new IntegerSerializer, new IntegerSerializer)
    val aggregateTopology = new AggregateTopology()


    //NOTE : You may find you need to play with these Config values in order
    //to get the stateful operation to work correctly/how you want it to
    //    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    //    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760.asInstanceOf[Object])
    //    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    //By playing around with these values you should be able to find the values that work for you
    //WARNING : Chaning these settings may have impact on the tests, as less frequent commits/state store
    //cache flushing may occur
    val testDriver = new TopologyTestDriver(aggregateTopology.createTopolgy(), props)

    //Aggregate
    testDriver.pipeInput(recordFactory.create("AggregateKeyInputTopic", 1, 1, 9995L))
    testDriver.pipeInput(recordFactory.create("AggregateKeyInputTopic", 1, 2, 9995L))
    testDriver.pipeInput(recordFactory.create("AggregateKeyInputTopic", 2, 3, 9997L))
    OutputVerifier.compareKeyValue(testDriver.readOutput("aggregateOutputTopic", integerDeserializer, longDeserializer),
      1.asInstanceOf[Integer],new lang.Long(1))
    OutputVerifier.compareKeyValue(testDriver.readOutput("aggregateOutputTopic", integerDeserializer, longDeserializer),
      1.asInstanceOf[Integer],new lang.Long(3))
    OutputVerifier.compareKeyValue(testDriver.readOutput("aggregateOutputTopic", integerDeserializer, longDeserializer),
      2.asInstanceOf[Integer],new lang.Long(3))
    val result1 = testDriver.readOutput("aggregateOutputTopic", integerDeserializer, longDeserializer)
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