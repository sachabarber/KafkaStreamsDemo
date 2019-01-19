package stateless.transformations

import java.io._
import java.util.Properties

import com.fasterxml.jackson.databind.deser.std.NumberDeserializers.BigIntegerDeserializer
import common.PropsHelper
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._

class GroupByTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = PropsHelper.createBasicStreamProperties("stateless-groupBy-application", "localhost:9092")
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
    val groupByTopology = new GroupByTopology()


    //NOTE : You may find you need to play with these Config values in order
    //to get the stateful operation to work correctly/how you want it to
    //    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    //    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760.asInstanceOf[Object])
    //    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    //By playing around with these values you should be able to find the values that work for you
    //WARNING : Chaning these settings may have impact on the tests, as less frequent commits/state store
    //cache flushing may occur
    val testDriver = new TopologyTestDriver(groupByTopology.createTopolgy(), props)

    //GroupByKey
    testDriver.pipeInput(recordFactory.create("GroupByKeyInputTopic", 1, "one", 9995L))
    testDriver.pipeInput(recordFactory.create("GroupByKeyInputTopic", 1, "one", 9995L))
    testDriver.pipeInput(recordFactory.create("GroupByKeyInputTopic", 2, "two", 9997L))
    OutputVerifier.compareKeyValue(testDriver.readOutput("groupedByKeyOutputTopic", integerDeserializer, stringDeserializer),
      1.asInstanceOf[Integer],"1")
    OutputVerifier.compareKeyValue(testDriver.readOutput("groupedByKeyOutputTopic", integerDeserializer, stringDeserializer),
      1.asInstanceOf[Integer],"2")
    OutputVerifier.compareKeyValue(testDriver.readOutput("groupedByKeyOutputTopic", integerDeserializer, stringDeserializer),
      2.asInstanceOf[Integer],"1")
    val result1 = testDriver.readOutput("groupedByKeyOutputTopic", integerDeserializer, stringDeserializer)
    assert(result1 == null)

    //GroupBy
    testDriver.pipeInput(recordFactory.create("GroupByInputTopic", 1, "one two three one two", 9998L))
    OutputVerifier.compareKeyValue(testDriver.readOutput("groupedByOutputTopic", stringDeserializer, stringDeserializer),
      "one","1")
    OutputVerifier.compareKeyValue(testDriver.readOutput("groupedByOutputTopic", stringDeserializer, stringDeserializer),
      "two","1")
    OutputVerifier.compareKeyValue(testDriver.readOutput("groupedByOutputTopic", stringDeserializer, stringDeserializer),
      "three","1")
    OutputVerifier.compareKeyValue(testDriver.readOutput("groupedByOutputTopic", stringDeserializer, stringDeserializer),
      "one","2")
    OutputVerifier.compareKeyValue(testDriver.readOutput("groupedByOutputTopic", stringDeserializer, stringDeserializer),
      "two","2")


    val result2 = testDriver.readOutput("groupedByOutputTopic", stringDeserializer, stringDeserializer)
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