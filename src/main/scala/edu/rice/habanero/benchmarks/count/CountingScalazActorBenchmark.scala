package edu.rice.habanero.benchmarks.count

import edu.rice.habanero.actors.{ScalazActor, ScalazActorState, ScalazPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object CountingScalazActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new CountingScalazActorBenchmark)
  }

  private final class CountingScalazActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      CountingConfig.parseArgs(args)
    }

    def printArgInfo() {
      CountingConfig.printArgs()
    }

    def runIteration() : Future[Boolean] = {
      val p = Promise[Boolean]

      val counter = new CountingActor()
      val producer = new ProducerActor(p, counter)
      
      counter.start()
      producer.start()

      producer.send(new IncrementMessage())

      return p.future
    }

    override def runAndVerify() : Boolean = {
      val f = runIteration()
      return Await.result(f, Duration.Inf)
    }
    
    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      ScalazActorState.awaitTermination()
      
      if (lastIteration) {
        ScalazPool.shutdown()
      }
    }
  }

  private class IncrementMessage()

  private class RetrieveMessage(sender: ScalazActor[AnyRef]) {
    def getSender() : ScalazActor[AnyRef] = {
      return sender
    }
  }

  private class ResultMessage(result: Int) {
    def getResult() : Int = {
      return result
    }
  }

  private class ProducerActor(completion: Promise[Boolean],
      counter: ScalazActor[AnyRef]) extends ScalazActor[AnyRef] {

    private val self = this

    override def process(msg: AnyRef) {
      msg match {
        case m: IncrementMessage =>

          var i = 0
          while (i < CountingConfig.N) {
            counter.send(m)
            i += 1
          }

          counter.send(new RetrieveMessage(self))

        case m: ResultMessage =>
          val result = m.getResult()
          if (result != CountingConfig.N) {
            println("ERROR: expected: " + CountingConfig.N + ", found: " + result)
          }
          completion.success(result == CountingConfig.N)
          exit()
      }
    }
  }

  private class CountingActor extends ScalazActor[AnyRef] {

    private var count = 0

    override def process(msg: AnyRef) {
      msg match {
        case m: IncrementMessage =>
          count += 1
        case m: RetrieveMessage =>
          m.getSender().send(new ResultMessage(count))
          exit()
      }
    }
  }

}
