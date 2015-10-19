package edu.rice.habanero.benchmarks.fjthrput

import akka.actor.{ActorRef, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import akka.actor.ActorSystem
import scala.concurrent.Promise
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.Await

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object ThroughputAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new ThroughputAkkaActorBenchmark)
  }

  private final class ThroughputAkkaActorBenchmark extends Benchmark {
    
    var system : ActorSystem = null
    
    def initialize(args: Array[String]) {
      ThroughputConfig.parseArgs(args)
    }

    def printArgInfo() {
      ThroughputConfig.printArgs()
    }

    def runIteration() : Future[List[Int]] = {
      system = AkkaActorState.newActorSystem("Throughput")
      implicit val ec = system.dispatcher

      val promises: List[Promise[Int]] = List.tabulate(ThroughputConfig.A)(x => Promise[Int])
      val futures = promises.map(x => x.future)
      
      val actors = Array.tabulate[ActorRef](ThroughputConfig.A)(i => {
        val loopActor = system.actorOf(Props(
            new ThroughputActor(promises(i), ThroughputConfig.N)))
        AkkaActorState.startActor(loopActor)
        loopActor
      })

      var m = 0
      while (m < ThroughputConfig.N) {
        actors.foreach(loopActor => {
          loopActor ! new Object()
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
      AkkaActorState.awaitTermination(system)
    }
  }

  private class ThroughputActor(completion: Promise[Int], totalMessages: Int)
      extends AkkaActor[AnyRef] {

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
