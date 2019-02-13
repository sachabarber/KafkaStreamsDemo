package joining

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import common.PropsHelper
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}
import scala.util.Try


class InnerJoinTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "inner-join-application", "localhost:9092")

  run()

  private def run(): Unit = {
    val topology = createTopolgy()
    val streams: KafkaStreams = new KafkaStreams(topology, props)
    streams.start()
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }

  def createTopolgy(): Topology = {

    val builder: StreamsBuilder = new StreamsBuilder
    val left: KStream[Int, String] =
      builder.stream[Int, String]("LeftTopic")
    val right: KStream[Int, String] =
      builder.stream[Int, String]("RightTopic")

    def tryToInt( s: String ) = Try(s.toInt).toOption

    //do the join
    val joinedStream = left.join(right)( (l: String, r: String) => {
      val result = (tryToInt(l), tryToInt(r))
      result match {
        case (Some(a), Some(b)) => a + b
        case (None, Some(b)) => b
        case (Some(a), None) => a
        case (None, None) => 0
      }
    } , JoinWindows.of(Duration.ofSeconds(2)))


    val peeked = joinedStream.peek((k,v)=> {

      val theK = k
      val theV = v
    })

    peeked.to("innerJoinOutput")

    builder.build()
  }
}


