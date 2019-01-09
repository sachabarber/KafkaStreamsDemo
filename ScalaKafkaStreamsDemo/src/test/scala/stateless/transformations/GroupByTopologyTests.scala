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
    val testDriver = new TopologyTestDriver(groupByTopology.createTopolgy(), props)

    //act
    testDriver.pipeInput(recordFactory.create("InputTopic", 1, "one two three one two", 9995L))

    //assert
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


    val result1 = testDriver.readOutput("groupedByOutputTopic", stringDeserializer, stringDeserializer)
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