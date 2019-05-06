package utils

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.Serdes


object Settings {

  val config = ConfigFactory.load("streamsApplication.conf")

  val kafkaConfig = config.getConfig("kafka")
  val zooKeepers = kafkaConfig.getString("zooKeepers")
  val bootStrapServers = kafkaConfig.getString("bootStrapServers")
  val partition = kafkaConfig.getInt("partition")
  val restApiDefaultHostName = kafkaConfig.getString("restApiDefaultHostName")
  val restApiDefaultPort = kafkaConfig.getInt("restApiDefaultPort")


  def createBasicProducerProperties(): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props
  }

  def createRatingStreamsProperties() : Properties = {
    val props = new Properties()
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(ProducerConfig.RETRIES_CONFIG, 0.asInstanceOf[Object])
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    // For illustrative purposes we disable record caches
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[Object])
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ratings-application")
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "ratings-application-client")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG,  s"${restApiDefaultHostName}:${restApiDefaultPort}")
    props.put(StreamsConfig.STATE_DIR_CONFIG, s"C:\\data\\kafka-streams".asInstanceOf[Object])
    props
  }

  private def createBasicStreamProperties() : Properties = {
    val props = new Properties()
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    // For illustrative purposes we disable record caches
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[Object])
    props.put(StreamsConfig.STATE_DIR_CONFIG, s"C:\\data\\kafka-streams".asInstanceOf[Object])
    props
  }

  def createBasicStreamProperties(applicationId: String, specificBootStrapServers: String) : Properties = {

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG,  "localhost:8080")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, if (specificBootStrapServers.isEmpty) bootStrapServers else specificBootStrapServers)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760.asInstanceOf[Object])
    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    props.put(StreamsConfig.STATE_DIR_CONFIG, s"C:\\data\\kafka-streams".asInstanceOf[Object])
    props
  }
}
