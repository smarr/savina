package edu.rice.habanero.benchmarks.concsll

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.concsll.SortedListConfig.{DoWorkMessage, EndWorkMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import som.Random

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SortedListJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SortedListJetlangActorBenchmark)
  }

  private final class SortedListJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SortedListConfig.parseArgs(args)
    }

    def printArgInfo() {
      SortedListConfig.printArgs()
    }

    def runIteration() : Future[Int] = {
      val p = Promise[Int]
      
      val numWorkers: Int = SortedListConfig.NUM_ENTITIES
      val numMessagesPerWorker: Int = SortedListConfig.NUM_MSGS_PER_WORKER

      val master = new Master(p, numWorkers, numMessagesPerWorker)
      master.start()

      return p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return SortedListConfig.verifyResult(n)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      JetlangActorState.awaitTermination()
      
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class Master(completion: Promise[Int], numWorkers: Int,
      numMessagesPerWorker: Int) extends JetlangActor[AnyRef] {

    private final val workers = new Array[JetlangActor[AnyRef]](numWorkers)
    private final val sortedList = new SortedList(completion)
    private var numWorkersTerminated: Int = 0

    override def onPostStart() {
      sortedList.start()

      var i: Int = 0
      while (i < numWorkers) {
        workers(i) = new Worker(this, sortedList, i + 1, numMessagesPerWorker)
        workers(i).start()
        workers(i).send(new DoWorkMessage())
        i += 1
      }
    }

    override def process(msg: AnyRef) {
      if (msg.isInstanceOf[SortedListConfig.EndWorkMessage]) {
        numWorkersTerminated += 1
        if (numWorkersTerminated == numWorkers) {
          sortedList.send(new EndWorkMessage())
          exit()
        }
      }
    }
  }

  private class Worker(master: Master, sortedList: SortedList, id: Int, numMessagesPerWorker: Int) extends JetlangActor[AnyRef] {

    private final val writePercent = SortedListConfig.WRITE_PERCENTAGE
    private final val sizePercent = SortedListConfig.SIZE_PERCENTAGE
    private var messageCount: Int = 0
    private final val random = new Random(id + numMessagesPerWorker + writePercent + sizePercent)

    override def process(msg: AnyRef) {
      messageCount += 1
      if (messageCount <= numMessagesPerWorker) {
        val anInt: Int = random.next(100)
        if (anInt < sizePercent) {
          sortedList.send(new SortedListConfig.SizeMessage(this))
        } else if (anInt < (sizePercent + writePercent)) {
          sortedList.send(new SortedListConfig.WriteMessage(this, random.next()))
        } else {
          sortedList.send(new SortedListConfig.ContainsMessage(this, random.next()))
        }
      } else {
        master.send(new EndWorkMessage())
        exit()
      }
    }
  }

  private class SortedList(completion: Promise[Int]) extends JetlangActor[AnyRef] {

    private[concsll] final val dataList = new SortedLinkedList[Integer]

    override def process(msg: AnyRef) {
      msg match {
        case writeMessage: SortedListConfig.WriteMessage =>
          val value: Int = writeMessage.value
          dataList.add(value)
          val sender = writeMessage.sender.asInstanceOf[JetlangActor[AnyRef]]
          sender.send(new SortedListConfig.ResultMessage(this, value))
        case containsMessage: SortedListConfig.ContainsMessage =>
          val value: Int = containsMessage.value
          val result: Int = if (dataList.contains(value)) 1 else 0
          val sender = containsMessage.sender.asInstanceOf[JetlangActor[AnyRef]]
          sender.send(new SortedListConfig.ResultMessage(this, result))
        case readMessage: SortedListConfig.SizeMessage =>
          val value: Int = dataList.size
          val sender = readMessage.sender.asInstanceOf[JetlangActor[AnyRef]]
          sender.send(new SortedListConfig.ResultMessage(this, value))
        case _: SortedListConfig.EndWorkMessage =>
          completion.success(dataList.size)
          exit()
        case _ =>
          System.err.println("Unsupported message: " + msg)
      }
    }
  }

}
