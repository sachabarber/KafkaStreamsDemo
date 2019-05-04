package utils

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.QueryableStoreType


import scala.concurrent.{ExecutionContext, Future}

object StateStores {
  val RATINGS_BY_EMAIL_STORE = "ratings-by-email-store"

  def waitUntilStoreIsQueryable[T]
  (
    storeName: String,
    queryableStoreType: QueryableStoreType[T],
    streams: KafkaStreams
  ) (implicit ec: ExecutionContext): Future[T] = {

    Retry.retry(5) {
      Thread.sleep(500)
      streams.store(storeName, queryableStoreType)

    }(ec)
  }


  @throws[InterruptedException]
  def waitUntilStoreIsQueryableSync[T](
        storeName: String,
        queryableStoreType: QueryableStoreType[T],
        streams: KafkaStreams): Option[T] = {
    while (true) {
      try {
        return Some(streams.store(storeName, queryableStoreType))
      }
      catch {
        case ignored: InvalidStateStoreException =>
          val state = streams.state
          // store not yet ready for querying
          Thread.sleep(100)
      }
    }
    None
  }
}
