package joining

import java.time.Duration
import java.util.Properties

import common.PropsHelper
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, Topology}

import scala.util.Try


class LeftJoinTopology extends App {

  import Serdes._

  val props: Properties = PropsHelper.createBasicStreamProperties(
    "left-join-application", "localhost:9092")

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

    //do the join
    val joinedStream = left.leftJoin(right)( (l: String, r: String) => {
      val result = s"Left='${l}', Right='${r}'"
      result
    } , JoinWindows.of(Duration.ofSeconds(2)))

    val peeked = joinedStream.peek((k,v)=> {

      val theK = k
      val theV = v
    })

    peeked.to("leftJoinOutput")

    builder.build()
  }
}


