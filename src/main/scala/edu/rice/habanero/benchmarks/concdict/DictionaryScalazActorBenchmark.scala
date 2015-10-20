package edu.rice.habanero.benchmarks.concdict

import edu.rice.habanero.actors.{ScalazActor, ScalazActorState, ScalazPool}
import edu.rice.habanero.benchmarks.concdict.DictionaryConfig.{DoWorkMessage, EndWorkMessage, ReadMessage, WriteMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import som.Random

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object DictionaryScalazActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new DictionaryScalazActorBenchmark)
  }

  private final class DictionaryScalazActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      DictionaryConfig.parseArgs(args)
    }

    def printArgInfo() {
      DictionaryConfig.printArgs()
    }

    def runIteration() : Future[Int] = {
      val p = Promise[Int]
      
      val numWorkers: Int = DictionaryConfig.NUM_ENTITIES
      val numMessagesPerWorker: Int = DictionaryConfig.NUM_MSGS_PER_WORKER

      val master = new Master(p, numWorkers, numMessagesPerWorker)
      master.start()

      return p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return DictionaryConfig.verifyResult(n)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      ScalazActorState.awaitTermination()
      if (lastIteration) {
        ScalazPool.shutdown()
      }
    }
  }

  private class Master(completion: Promise[Int], numWorkers: Int,
      numMessagesPerWorker: Int) extends ScalazActor[AnyRef] {

    private final val workers = new Array[ScalazActor[AnyRef]](numWorkers)
    private final val dictionary = new Dictionary(completion)
    private var numWorkersTerminated: Int = 0

    override def onPostStart() {
      dictionary.start()

      var i: Int = 0
      while (i < numWorkers) {
        workers(i) = new Worker(this, dictionary, i, numMessagesPerWorker)
        workers(i).start()
        workers(i).send(new DoWorkMessage())
        i += 1
      }
    }

    override def process(msg: AnyRef) {
      if (msg.isInstanceOf[DictionaryConfig.EndWorkMessage]) {
        numWorkersTerminated += 1
        if (numWorkersTerminated == numWorkers) {
          dictionary.send(new EndWorkMessage())
          exit()
        }
      }
    }
  }

  private class Worker(master: Master, dictionary: Dictionary, id: Int, numMessagesPerWorker: Int) extends ScalazActor[AnyRef] {

    private final val writePercent = DictionaryConfig.WRITE_PERCENTAGE
    private var messageCount: Int = 0
    private final val random = new util.Random(id + numMessagesPerWorker + writePercent)

    override def process(msg: AnyRef) {
      messageCount += 1
      if (messageCount <= numMessagesPerWorker) {
        val anInt: Int = random.nextInt(100)
        if (anInt < writePercent) {
          dictionary.send(new WriteMessage(this, random.nextInt, random.nextInt))
        } else {
          dictionary.send(new ReadMessage(this, random.nextInt))
        }
      } else {
        master.send(new EndWorkMessage())
        exit()
      }
    }
  }

  private class Dictionary(completion: Promise[Int]) extends ScalazActor[AnyRef] {

    private[concdict] final val dataMap = DictionaryConfig.createDataMap(
        DictionaryConfig.DATA_LIMIT)

    override def process(msg: AnyRef) {
      msg match {
        case writeMessage: DictionaryConfig.WriteMessage =>
          val key = writeMessage.key
          val value = writeMessage.value
          dataMap.put(key, value)
          val sender = writeMessage.sender.asInstanceOf[ScalazActor[AnyRef]]
          sender.send(new DictionaryConfig.ResultMessage(this, value))
        case readMessage: DictionaryConfig.ReadMessage =>
          val value = dataMap.get(readMessage.key)
          val sender = readMessage.sender.asInstanceOf[ScalazActor[AnyRef]]
          sender.send(new DictionaryConfig.ResultMessage(this, value))
        case _: DictionaryConfig.EndWorkMessage =>
          completion.success(dataMap.size)
          exit()
        case _ =>
          System.err.println("Unsupported message: " + msg)
      }
    }
  }
}
