package stateful.transformations.aggregating

import java.io._
import java.lang
import java.util.Properties

import org.apache.kafka.common.serialization.{IntegerDeserializer, LongDeserializer, _}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._
import utils.Settings

class ReduceTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = Settings.createBasicStreamProperties("stateless-reduce-application", "localhost:9092")
  val stringDeserializer: StringDeserializer = new StringDeserializer
  val integerDeserializer: IntegerDeserializer = new IntegerDeserializer

  before {
  }

  after {
  }


  test("Should produce correct output") {

    //arrange
    val recordFactory: ConsumerRecordFactory[java.lang.String, java.lang.String] =
      new ConsumerRecordFactory[java.lang.String, java.lang.String](new StringSerializer, new StringSerializer)
    val reduceTopology = new ReduceTopology()


    //NOTE : You may find you need to play with these Config values in order
    //to get the stateful operation to work correctly/how you want it to
    //    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    //    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760.asInstanceOf[Object])
    //    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    //By playing around with these values you should be able to find the values that work for you
    //WARNING : Chaning these settings may have impact on the tests, as less frequent commits/state store
    //cache flushing may occur
    val testDriver = new TopologyTestDriver(reduceTopology.createTopolgy(), props)

    //Aggregate
    testDriver.pipeInput(recordFactory.create("ReduceInputTopic", "alice", "E", 9995L))
    testDriver.pipeInput(recordFactory.create("ReduceInputTopic", "bob", "A", 9996L))
    testDriver.pipeInput(recordFactory.create("ReduceInputTopic", "charlie", "A", 9997L))

    OutputVerifier.compareKeyValue(testDriver.readOutput("ReduceOutputTopic", stringDeserializer, integerDeserializer),
      "E",new lang.Integer(5))
    OutputVerifier.compareKeyValue(testDriver.readOutput("ReduceOutputTopic", stringDeserializer, integerDeserializer),
      "A",new lang.Integer(3))
    OutputVerifier.compareKeyValue(testDriver.readOutput("ReduceOutputTopic", stringDeserializer, integerDeserializer),
      "A",new lang.Integer(10))

    val result1 = testDriver.readOutput("ReduceOutputTopic", stringDeserializer, integerDeserializer)
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