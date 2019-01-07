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
    List(0L,1L,2L).foreach(x => {
      testDriver.pipeInput(recordFactory.create("InputTopic", "key", java.lang.Long.valueOf(x), 9995L + 1))
    })

    //assert
    OutputVerifier.compareValue(testDriver.readOutput("OutputTopic", stringDeserializer, longDeserializer), 0L.asInstanceOf[java.lang.Long])
    OutputVerifier.compareValue(testDriver.readOutput("OutputTopic", stringDeserializer, longDeserializer), 2L.asInstanceOf[java.lang.Long])
    val result = testDriver.readOutput("OddTopic", stringDeserializer, longDeserializer)
    assert(result == null)

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


  //++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  // https://jaceklaskowski.gitbooks.io/mastering-kafka-streams/kafka-logging.html


//
//
//  test("UsingMatcher should be cool") {
//    //equality examples
//    Array(1, 2) should equal (Array(1, 2))
//    val resultInt = 3
//    resultInt should equal (3) // can customize equality
//    resultInt should === (3)   // can customize equality and enforce type constraints
//    resultInt should be (3)    // cannot customize equality, so fastest to compile
//    resultInt shouldEqual 3    // can customize equality, no parentheses required
//    resultInt shouldBe 3       // cannot customize equality, so fastest to compile, no parentheses required
//
//    //length examples
//    List(1,2) should have length 2
//    "cat" should have length 3
//
//    //string examples
//    val helloWorld = "Hello world"
//    helloWorld should startWith ("Hello")
//    helloWorld should endWith ("world")
//
//    val sevenString ="six seven eight"
//    sevenString should include ("seven")
//
//    //greater than / less than
//    val one = 1
//    val zero = 0
//    val seven = 7
//    one should be < seven
//    one should be > zero
//    one should be <= seven
//    one should be >= zero
//
//    //emptiness
//    List() shouldBe empty
//    List(1,2) should not be empty
//  }


}