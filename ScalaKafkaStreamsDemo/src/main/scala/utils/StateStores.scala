package utils

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.{QueryableStoreType, QueryableStoreTypes}

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


  private def printStoreMetaData[K, V](streams:KafkaStreams, storeName:String) : Unit = {

    val md = streams.allMetadata()
    val mdStore = streams.allMetadataForStore(storeName)

    val maybeStore = StateStores.waitUntilStoreIsQueryableSync(
      storeName,
      QueryableStoreTypes.keyValueStore[K,V](),
      streams)

    maybeStore match {
      case Some(store) => {
        val range = store.all
        val HASNEXT = range.hasNext
        while (range.hasNext) {
          val next = range.next
          System.out.print(s"key: ${next.key} value: ${next.value}")
        }
      }
      case None => {
        System.out.print(s"store not ready")
        throw new Exception("not ready")
      }
    }
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
