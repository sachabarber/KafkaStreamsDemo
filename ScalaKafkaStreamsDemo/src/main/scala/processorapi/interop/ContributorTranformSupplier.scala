package processorapi.interop

import java.time.Duration

import entities.Contributor
import org.apache.kafka.streams.kstream.{ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.processor.{PunctuationType, Punctuator}

class ContributorTranformSupplier extends ValueTransformerSupplier[Contributor, Contributor] {
  override def get(): ValueTransformer[Contributor, Contributor] = new ValueTransformer[Contributor, Contributor] {

    import org.apache.kafka.streams.processor.ProcessorContext
    import org.apache.kafka.streams.state.KeyValueStore

    private var context:ProcessorContext  = null
    private var contributorStore:KeyValueStore[String, Contributor]  = null

    override def init(context: ProcessorContext): Unit = {
      import org.apache.kafka.streams.state.KeyValueStore
      this.context = context
      this.contributorStore = context.getStateStore("contributorStore")
        .asInstanceOf[KeyValueStore[String, Contributor]]

      //to punctuate you would do something like this
      //      context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator {
      //        override def punctuate(timestamp: Long): Unit = {
      //
      //          val it = contributorStore.all
      //          val currentTime = System.currentTimeMillis
      //          while (it.hasNext) {
      //            val contributorValue = it.next.value
      //            if (contributorValue.updatedWithinLastMillis(currentTime, 11000))
      //              context.forward(contributorValue.email,contributorValue)
      //          }
      //        }
      //      })
    }

    override def transform(value: Contributor): Contributor = {

      var finalContributor:Contributor = null
      try {
        val contributor = contributorStore.get(value.email)
        if(contributor == null) {
          contributorStore.putIfAbsent(value.email, value)
          finalContributor = value
        }
        else {
          val newContributor = contributor.copy(
            ranking = contributor.ranking + 1,
            lastUpdatedTime = System.currentTimeMillis
          )
          contributorStore.put(value.email,newContributor)
          finalContributor = newContributor
        }

        finalContributor
      }
      catch {
        case e:NullPointerException => {
          contributorStore.putIfAbsent(value.email, value)
          value
        }
        case e:Exception => {
          value
        }
      }
    }

    override def close(): Unit = {
      if(contributorStore != null) {
        contributorStore.close()
      }

    }
  }
}


//
//val transformerSupplierJ: TransformerSupplier[K, V, KeyValue[K1, V1]] = () => {
//  val transformerS: Transformer[K, V, (K1, V1)] = transformerSupplier()
//  new Transformer[K, V, KeyValue[K1, V1]] {
//  override def transform(key: K, value: V): KeyValue[K1, V1] = {
//  val res = transformerS.transform(key, value)
//  new KeyValue[K1, V1](res._1, res._2)
//}
//
//  override def init(context: ProcessorContext): Unit = transformerS.init(context)
//
//  override def punctuate(timestamp: Long): KeyValue[K1, V1] = {
//  val res = transformerS.punctuate(timestamp)
//  new KeyValue[K1, V1](res._1, res._2)
//}
//
//  override def close(): Unit = transformerS.close()
//}
//}
