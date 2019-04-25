package stateless.transformations

import java.io._
import java.util.Properties

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest._
import utils.Settings

class BranchTopologyTests
  extends FunSuite
  with BeforeAndAfter
  with Matchers {

  val props = Settings.createBasicStreamProperties("stateless-branch-application", "localhost:9092")
  val stringDeserializer: StringDeserializer = new StringDeserializer

  before {
  }

  after {
  }


  test("Should produce correct output") {

    //arrange
    val recordFactory: ConsumerRecordFactory[String, String] =
      new ConsumerRecordFactory[String, String](new StringSerializer, new StringSerializer)
    val branchTopology = new BranchTopology()
    val testDriver = new TopologyTestDriver(branchTopology.createTopolgy(), props)

    //act
    val consumerRecord1 = recordFactory.create("InputTopic", "key", "Odd line1", 9995L)
    testDriver.pipeInput(consumerRecord1)
    val consumerRecord2 = recordFactory.create("InputTopic", "key", "Odd line2", 9996L)
    testDriver.pipeInput(consumerRecord2)
    val consumerRecord3 = recordFactory.create("InputTopic", "key", "Even line1", 9997L)
    testDriver.pipeInput(consumerRecord3)

    //assert
    OutputVerifier.compareKeyValue(testDriver.readOutput("OddTopic", stringDeserializer, stringDeserializer), "key", "Odd line1")
    OutputVerifier.compareKeyValue(testDriver.readOutput("OddTopic", stringDeserializer, stringDeserializer), "key", "Odd line2")
    val result = testDriver.readOutput("OddTopic", stringDeserializer, stringDeserializer)
    assert(result == null)

    OutputVerifier.compareKeyValue(testDriver.readOutput("EvenTopic", stringDeserializer, stringDeserializer), "key", "Even line1")
    val result2 = testDriver.readOutput("EvenTopic", stringDeserializer, stringDeserializer)
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