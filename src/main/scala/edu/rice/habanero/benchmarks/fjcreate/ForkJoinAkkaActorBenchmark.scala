package edu.rice.habanero.benchmarks.fjcreate

import akka.actor.Props
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import akka.actor.ActorSystem
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ForkJoinAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ForkJoinAkkaActorBenchmark)
  }

  private final class ForkJoinAkkaActorBenchmark extends Benchmark {
    var system: ActorSystem = null
    
    def initialize(args: Array[String]) {
      ForkJoinConfig.parseArgs(args)
    }

    def printArgInfo() {
      ForkJoinConfig.printArgs()
    }

    def runIteration() : Future[List[Double]] = {
      system = AkkaActorState.newActorSystem("ForkJoin")
      implicit val ec = system.dispatcher

      val promises: List[Promise[Double]] = List.tabulate(ForkJoinConfig.N)(
          x => Promise[Double])
      
      promises.foreach(p => {
        val fjRunner = system.actorOf(Props(new ForkJoinActor(p)))
        AkkaActorState.startActor(fjRunner)
        fjRunner ! new Object()
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
      AkkaActorState.awaitTermination(system)
    }
  }

  private class ForkJoinActor(completion: Promise[Double]) extends AkkaActor[AnyRef] {
    override def process(msg: AnyRef) {
      completion.success(ForkJoinConfig.performComputation(37.2))
      exit()
    }
  }

}
