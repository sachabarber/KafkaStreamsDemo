package processorapi.interop

import java.io.PrintWriter

import entities.Contributor
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier}

class ContributorPrintingSupplier(val pw: PrintWriter) extends ProcessorSupplier[String, Long] {
  override def get(): Processor[String, Long] = new Processor[String,Long] {

    import org.apache.kafka.streams.processor.ProcessorContext
    import org.apache.kafka.streams.state.KeyValueStore

    private var context:ProcessorContext  = null
    private var contributorStore:KeyValueStore[String, Long]  = null

    override def init(context: ProcessorContext): Unit = {
      import org.apache.kafka.streams.state.KeyValueStore
      this.context = context
      this.contributorStore = context.getStateStore("processContributorStore")
        .asInstanceOf[KeyValueStore[String, Long]]
    }

    override def process(key: String, value: Long): Unit = {
      pw.write(s"key ${key} has been seen ${value} times\r\n")
      pw.flush()
    }

    override def close(): Unit = {
      if(contributorStore != null) {
        contributorStore.close()
      }
    }
  }
}

