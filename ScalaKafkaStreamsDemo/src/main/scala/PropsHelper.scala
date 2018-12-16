import java.util.Properties
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.StreamsConfig


object PropsHelper  {

  def createBasicStreamProperties(applicationId: String, bootStrapServers: String) : Properties = {

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000.asInstanceOf[Object])
    // For illustrative purposes we disable record caches
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[Object])
    props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 50000.asInstanceOf[Object])
    props.put(StreamsConfig.STATE_DIR_CONFIG, s"C:\\data\\kafka-streams".asInstanceOf[Object])
    props
  }
}
