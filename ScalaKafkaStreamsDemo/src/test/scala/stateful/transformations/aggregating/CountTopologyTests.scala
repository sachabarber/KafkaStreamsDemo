package stateful.transformations.aggregating

import java.io._
import java.lang
import java.util.Properties

import org.apache.kafka.common.serialization.{LongDeserializer, _}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._
import utils.Settings

class CountTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = Settings.createBasicStreamProperties("stateless-count-application", "localhost:9092")
  val stringDeserializer: StringDeserializer = new StringDeserializer
  val longDeserializer: LongDeserializer = new LongDeserializer

  before {
  }

  after {
  }


  test("Should produce correct output") {

    //arrange
    val recordFactory: ConsumerRecordFactory[java.lang.String, java.lang.String] =
      new ConsumerRecordFactory[java.lang.String, java.lang.String](new StringSerializer, new StringSerializer)
    val countTopology = new CountTopology()


    //NOTE : You may find you need to play with these Config values in order
    //to get the stateful operation to work correctly/how you want it to
    //    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    //    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760.asInstanceOf[Object])
    //    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    //By playing around with these values you should be able to find the values that work for you
    //WARNING : Chaning these settings may have impact on the tests, as less frequent commits/state store
    //cache flushing may occur
    val testDriver = new TopologyTestDriver(countTopology.createTopolgy(), props)

    //Aggregate
    testDriver.pipeInput(recordFactory.create("CountInputTopic", "one", "one two three one two four", 9995L))
    testDriver.pipeInput(recordFactory.create("CountInputTopic", "two", "one two", 9997L))
    OutputVerifier.compareKeyValue(testDriver.readOutput("WordsWithCountsOutputTopic", stringDeserializer, longDeserializer),
      "one",new lang.Long(1))
    OutputVerifier.compareKeyValue(testDriver.readOutput("WordsWithCountsOutputTopic", stringDeserializer, longDeserializer),
      "two",new lang.Long(1))
    OutputVerifier.compareKeyValue(testDriver.readOutput("WordsWithCountsOutputTopic", stringDeserializer, longDeserializer),
      "three",new lang.Long(1))
    OutputVerifier.compareKeyValue(testDriver.readOutput("WordsWithCountsOutputTopic", stringDeserializer, longDeserializer),
      "one",new lang.Long(2))
    OutputVerifier.compareKeyValue(testDriver.readOutput("WordsWithCountsOutputTopic", stringDeserializer, longDeserializer),
      "two",new lang.Long(2))
    OutputVerifier.compareKeyValue(testDriver.readOutput("WordsWithCountsOutputTopic", stringDeserializer, longDeserializer),
      "four",new lang.Long(1))

    OutputVerifier.compareKeyValue(testDriver.readOutput("WordsWithCountsOutputTopic", stringDeserializer, longDeserializer),
      "one",new lang.Long(3))
    OutputVerifier.compareKeyValue(testDriver.readOutput("WordsWithCountsOutputTopic", stringDeserializer, longDeserializer),
      "two",new lang.Long(3))

    val result1 = testDriver.readOutput("WordsWithCountsOutputTopic", stringDeserializer, stringDeserializer)
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