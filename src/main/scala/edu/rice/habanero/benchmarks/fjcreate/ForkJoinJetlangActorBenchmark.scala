package edu.rice.habanero.benchmarks.fjcreate

import edu.rice.habanero.actors.{JetlangActor, JetlangActorState, JetlangPool}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration


/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ForkJoinJetlangActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ForkJoinJetlangActorBenchmark)
  }

  private final class ForkJoinJetlangActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      ForkJoinConfig.parseArgs(args)
    }

    def printArgInfo() {
      ForkJoinConfig.printArgs()
    }

    def runIteration() : Future[List[Double]] = {
      import ExecutionContext.Implicits.global
      
      val promises: List[Promise[Double]] = List.tabulate(ForkJoinConfig.N)(
          x => Promise[Double])
      
      promises.foreach(p => {
        val fjRunner = new ForkJoinActor(p)
        fjRunner.start()
        fjRunner.send(new Object())
      })

      val futures = promises.map(x => x.future)
      return Future.sequence(futures)
    }
    
    override def runAndVerify() : Boolean = {
      import ExecutionContext.Implicits.global
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      val expResult = ForkJoinConfig.performComputation(37.2)
      n.foreach { x => 
        if (x != expResult) { return false } }
      return true
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      JetlangActorState.awaitTermination()
      
      if (lastIteration) {
        JetlangPool.shutdown()
      }
    }
  }

  private class ForkJoinActor(completion: Promise[Double]) extends JetlangActor[AnyRef] {
    override def process(msg: AnyRef) {
      completion.success(ForkJoinConfig.performComputation(37.2))
      exit()
    }
  }
}
