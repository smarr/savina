package edu.rice.habanero.benchmarks.bndbuffer

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.bndbuffer.ProdConsBoundedBufferConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import scala.collection.mutable.ListBuffer
import akka.actor.ActorSystem

/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ProdConsAkkaActorBenchmark {
  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ProdConsAkkaActorBenchmark)
  }

  private final class ProdConsAkkaActorBenchmark extends Benchmark {
    var system: ActorSystem = null
    
    def initialize(args: Array[String]) {
      ProdConsBoundedBufferConfig.parseArgs(args)
    }

    def printArgInfo() {
      ProdConsBoundedBufferConfig.printArgs()
    }

    def runIteration() : Future[Double] = {
      system = AkkaActorState.newActorSystem("ProdCons")
      val p = Promise[Double]

      val manager = system.actorOf(Props(
        new ManagerActor(p,
          ProdConsBoundedBufferConfig.bufferSize,
          ProdConsBoundedBufferConfig.numProducers,
          ProdConsBoundedBufferConfig.numConsumers,
          ProdConsBoundedBufferConfig.numItemsPerProducer)),
        name = "manager")

      AkkaActorState.startActor(manager)

      return p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return ProdConsBoundedBufferConfig.verifyResult(n)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }

    private class ManagerActor(completion: Promise[Double], bufferSize: Int,
        numProducers: Int, numConsumers: Int,
        numItemsPerProducer: Int) extends AkkaActor[AnyRef] {

      private val adjustedBufferSize: Int = bufferSize - numProducers
      private val availableProducers = new ListBuffer[ActorRef]
      private val availableConsumers = new ListBuffer[ActorRef]
      private val pendingData = new ListBuffer[ProdConsBoundedBufferConfig.DataItemMessage]
      private var numTerminatedProducers: Int = 0
      private var dataSum: Double = 0.0

      private val producers = Array.tabulate[ActorRef](numProducers)(i =>
        context.system.actorOf(Props(new ProducerActor(i, self, numItemsPerProducer))))
      private val consumers = Array.tabulate[ActorRef](numConsumers)(i =>
        context.system.actorOf(Props(new ConsumerActor(i, self))))

      override def onPostStart() {
        consumers.foreach(loopConsumer => {
          availableConsumers.append(loopConsumer)
          AkkaActorState.startActor(loopConsumer)
        })

        producers.foreach(loopProducer => {
          AkkaActorState.startActor(loopProducer)
        })

        producers.foreach(loopProducer => {
          loopProducer ! new ProduceDataMessage()
        })
      }

      override def onPreExit() {
        consumers.foreach(loopConsumer => {
          loopConsumer ! new ConsumerExitMessage()
        })
      }

      override def process(theMsg: AnyRef) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            dataSum += dm.data
            val producer: ActorRef = dm.producer.asInstanceOf[ActorRef]
            if (availableConsumers.isEmpty) {
              pendingData.append(dm)
            } else {
              availableConsumers.remove(0) ! dm
            }
            if (pendingData.size >= adjustedBufferSize) {
              availableProducers.append(producer)
            } else {
              producer ! new ProduceDataMessage()
            }
          case cm: ProdConsBoundedBufferConfig.ConsumerAvailableMessage =>
            val consumer: ActorRef = cm.consumer.asInstanceOf[ActorRef]
            if (pendingData.isEmpty) {
              availableConsumers.append(consumer)
              tryExit()
            } else {
              consumer ! pendingData.remove(0)
              if (!availableProducers.isEmpty) {
                availableProducers.remove(0) ! new ProduceDataMessage()
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

    private class ProducerActor(id: Int, manager: ActorRef,
        numItemsToProduce: Int) extends AkkaActor[AnyRef] {

      private var prodItem: Double = 0.0
      private var itemsProduced: Int = 0

      private def produceData() {
        prodItem = processItem(prodItem, prodCost)
        manager ! new ProdConsBoundedBufferConfig.DataItemMessage(prodItem, self)
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
        manager ! new ProducerExitMessage()
      }
    }

    private class ConsumerActor(id: Int, manager: ActorRef) extends AkkaActor[AnyRef] {

      private val consumerAvailableMessage = new ProdConsBoundedBufferConfig.ConsumerAvailableMessage(self)
      private var consItem: Double = 0

      protected def consumeDataItem(dataToConsume: Double) {
        consItem = processItem(consItem + dataToConsume, consCost)
      }

      override def process(theMsg: AnyRef) {
        theMsg match {
          case dm: ProdConsBoundedBufferConfig.DataItemMessage =>
            consumeDataItem(dm.data)
            manager ! consumerAvailableMessage
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
