package edu.rice.habanero.benchmarks.fjthrput

import edu.rice.habanero.actors.{ScalazActor, ScalazActorState, ScalazPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThroughputScalazActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThroughputScalazActorBenchmark)
  }

  private final class ThroughputScalazActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ThroughputConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThroughputConfig.printArgs()
    }

    def runIteration() : Future[List[Int]] = {
      import ExecutionContext.Implicits.global
      
      val promises: List[Promise[Int]] = List.tabulate(ThroughputConfig.A)(x => Promise[Int])
      val futures = promises.map(x => x.future)
      
      val actors = Array.tabulate[ThroughputActor](ThroughputConfig.A)(i => {
        val loopActor = new ThroughputActor(promises(i), ThroughputConfig.N)
        loopActor.start()
        loopActor
      })

      var m = 0
      while (m < ThroughputConfig.N) {

        actors.foreach(loopActor => {
          loopActor.send(new Object())
        })

        m += 1
      }

      return Future.sequence(futures)
    }

    override def runAndVerify() : Boolean = {
      import ExecutionContext.Implicits.global
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      n.foreach { x => 
        if (x != ThroughputConfig.N) { return false } }
      return true
    }
    
    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      ScalazActorState.awaitTermination()
      if (lastIteration) {
        ScalazPool.shutdown()
      }
    }
  }

  private class ThroughputActor(completion: Promise[Int], totalMessages: Int) extends ScalazActor[AnyRef] {

    private var messagesProcessed = 0

    override def process(msg: AnyRef) {
      messagesProcessed += 1
      ThroughputConfig.performComputation(37.2)

      if (messagesProcessed == totalMessages) {
        completion.success(messagesProcessed)
        exit()
      }
    }
  }
}
