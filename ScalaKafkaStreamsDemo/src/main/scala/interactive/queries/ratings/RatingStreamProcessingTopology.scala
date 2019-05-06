package interactive.queries.ratings


import java.util

import entities.Rating
import org.apache.kafka.streams.scala.{Serdes, _}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.state.KeyValueStore
import serialization.JSONSerde
import utils.StateStores


class RatingStreamProcessingTopology  {

  def createTopolgy(): Topology = {

    implicit val stringSerde = Serdes.String
    implicit val ratingSerde = new JSONSerde[Rating]
    implicit val listRatingSerde = new JSONSerde[List[Rating]]
    implicit val consumed = kstream.Consumed.`with`(stringSerde, ratingSerde)
    implicit val materializer = Materialized.`with`(stringSerde, listRatingSerde)
    implicit val grouped = Grouped.`with`(stringSerde, ratingSerde)

    val builder: StreamsBuilder = new StreamsBuilder
    val ratings: KStream[String, Rating] =
      builder.stream[String, Rating]("rating-submit-topic")


    import org.apache.kafka.streams.state.Stores

    val logConfig = new util.HashMap[String, String]
    logConfig.put("retention.ms", "172800000")
    logConfig.put("retention.bytes", "10000000000")
    logConfig.put("cleanup.policy", "compact,delete")
    val ratingByEmailStoreName = StateStores.RATINGS_BY_EMAIL_STORE
    val ratingByEmailStoreSupplied = Stores.inMemoryKeyValueStore(ratingByEmailStoreName)
    val ratingByEmailStoreBuilder = Stores.keyValueStoreBuilder(ratingByEmailStoreSupplied,
      Serdes.String, listRatingSerde)
      .withLoggingEnabled(logConfig)
      .withCachingEnabled()

    val builtStore = ratingByEmailStoreBuilder.build()

    //When aggregating a grouped stream, you must provide an initializer (e.g., aggValue = 0)
    //and an “adder” aggregator (e.g., aggValue + curValue). When aggregating a grouped table,
    //you must provide a “subtractor” aggregator (think: aggValue - oldValue).
    val groupedBy = ratings.groupByKey

    //aggrgate by (user email -> their ratings)
    val ratingTable : KTable[String, List[Rating]] = groupedBy
      .aggregate[List[Rating]](List[Rating]())((aggKey, newValue, aggValue) => {
      newValue :: aggValue
    })(Materialized.as[String, List[Rating]](ratingByEmailStoreSupplied))

    ratingTable.mapValues((k,v) => {
      val theKey = k
      val theValue = v
      v
    })


    builder.build()
  }
}

