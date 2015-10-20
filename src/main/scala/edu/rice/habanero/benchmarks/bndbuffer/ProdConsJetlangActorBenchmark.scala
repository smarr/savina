package edu.rice.habanero.benchmarks.bndbuffer

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.bndbuffer.ProdConsBoundedBufferConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import scala.collection.mutable.ListBuffer

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ProdConsJetlangActorBenchmark {
  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ProdConsJetlangActorBenchmark)
  }

  private final class ProdConsJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ProdConsBoundedBufferConfig.parseArgs(args)
    }

    def printArgInfo() {
      ProdConsBoundedBufferConfig.printArgs()
    }

    def runIteration() : Future[Double] = {
      val p = Promise[Double]
      
      val manager = new ManagerActor(p,
        ProdConsBoundedBufferConfig.bufferSize,
        ProdConsBoundedBufferConfig.numProducers,
        ProdConsBoundedBufferConfig.numConsumers,
        ProdConsBoundedBufferConfig.numItemsPerProducer)

      manager.start()

      return p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return ProdConsBoundedBufferConfig.verifyResult(n)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      JetlangActorState.awaitTermination()
      
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }

    private class ManagerActor(completion: Promise[Double], bufferSize: Int,
        numProducers: Int, numConsumers: Int,
        numItemsPerProducer: Int) extends JetlangActor[AnyRef] {


      private val adjustedBufferSize: Int = bufferSize - numProducers
      private val availableProducers = new ListBuffer[ProducerActor]
      private val availableConsumers = new ListBuffer[ConsumerActor]
      private val pendingData = new ListBuffer[ProdConsBoundedBufferConfig.DataItemMessage]
      private var numTerminatedProducers: Int = 0
      private var dataSum: Double = 0.0

      private val producers = Array.tabulate[ProducerActor](numProducers)(i =>
        new ProducerActor(i, this, numItemsPerProducer))
      private val consumers = Array.tabulate[ConsumerActor](numConsumers)(i =>
        new ConsumerActor(i, this))

      override def onPostStart() {
        consumers.foreach(loopConsumer => {
          availableConsumers.append(loopConsumer)
          loopConsumer.start()
        })

        producers.foreach(loopProducer => {
          loopProducer.start()
          loopProducer.send(new ProduceDataMessage())
        })
      }

      override def onPreExit() {
        consumers.foreach(loopConsumer => {
          loopConsumer.send(new ConsumerExitMessage())
        })
      }

      override def process(theMsg: AnyRef) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            dataSum += dm.data
            val producer: ProducerActor = dm.producer.asInstanceOf[ProducerActor]
            if (availableConsumers.isEmpty) {
              pendingData.append(dm)
            } else {
              availableConsumers.remove(0).send(dm)
            }
            if (pendingData.size >= adjustedBufferSize) {
              availableProducers.append(producer)
            } else {
              producer.send(new ProduceDataMessage())
            }
          case cm: ProdConsBoundedBufferConfig.ConsumerAvailableMessage =>
            val consumer: ConsumerActor = cm.consumer.asInstanceOf[ConsumerActor]
            if (pendingData.isEmpty) {
              availableConsumers.append(consumer)
              tryExit()
            } else {
              consumer.send(pendingData.remove(0))
              if (!availableProducers.isEmpty) {
                availableProducers.remove(0).send(new ProduceDataMessage())
              }
            }
          case _: ProdConsBoundedBufferConfig.ProducerExitMessage =>
            numTerminatedProducers += 1
            tryExit()
          case msg =>
            val ex = new IllegalArgumentException("Unsupported message: " + msg)
            ex.printStackTrace(System.err)
        }
      }

      def tryExit() {
        if (numTerminatedProducers == numProducers &&
            availableConsumers.size == numConsumers) {
          completion.success(dataSum)
          exit()
        }
      }
    }

    private class ProducerActor(id: Int, manager: ManagerActor, numItemsToProduce: Int) extends JetlangActor[AnyRef] {

      private var prodItem: Double = 0.0
      private var itemsProduced: Int = 0

      private def produceData() {
        prodItem = processItem(prodItem, prodCost)
        manager.send(new ProdConsBoundedBufferConfig.DataItemMessage(prodItem, this))
        itemsProduced += 1
      }

      override def process(theMsg: AnyRef) {
        if (theMsg.isInstanceOf[ProdConsBoundedBufferConfig.ProduceDataMessage]) {
          if (itemsProduced == numItemsToProduce) {
            exit()
          } else {
            produceData()
          }
        } else {
          val ex = new IllegalArgumentException("Unsupported message: " + theMsg)
          ex.printStackTrace(System.err)
        }
      }

      override def onPreExit() {
        manager.send(new ProducerExitMessage())
      }
    }

    private class ConsumerActor(id: Int, manager: ManagerActor) extends JetlangActor[AnyRef] {

      private val consumerAvailableMessage = new ProdConsBoundedBufferConfig.ConsumerAvailableMessage(this)
      private var consItem: Double = 0

      protected def consumeDataItem(dataToConsume: Double) {
        consItem = processItem(consItem + dataToConsume, consCost)
      }

      override def process(theMsg: AnyRef) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            consumeDataItem(dm.data)
            manager.send(consumerAvailableMessage)
          case _: ProdConsBoundedBufferConfig.ConsumerExitMessage =>
            exit()
          case msg =>
            val ex = new IllegalArgumentException("Unsupported message: " + msg)
            ex.printStackTrace(System.err)
        }
      }
    }
  }
}
