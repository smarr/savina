package edu.rice.habanero.benchmarks.pingpong

import edu.rice.habanero.benchmarks.BenchmarkRunner
import som.actorsflush.Promise
import som.actorsflush.Actor
import edu.rice.habanero.benchmarks.Benchmark
import som.actorsflush.EventualMessage
import som.actorsflush.Promise.Resolver
import som.actorsflush.EventualMessage.DirectMessage
import som.actorsflush.Promise.ResolutionAction

object PingPongSomFlushBenchmark {
  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new PingPongSomBenchmark)
  }
  
  private final class PingPongSomBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      PingPongConfig.parseArgs(args)
    }

    def printArgInfo() {
      PingPongConfig.printArgs()
    }
    
    def runIteration() : Promise = {
      val p = new Promise(null)
      
      val pong = new PongActor(new Resolver(p))
      val ping = new PingActor(PingPongConfig.N, pong)
      
      ping.send(new StartMessage(ping));
      
      return p
    }
    
    override def runAndVerify() : Boolean = {
      val p = runIteration();
      
      class Complete extends ResolutionAction {
        def action(result: Object) {
          p.notify()
        }
      }
      
      p.whenResolved(new Complete)
      val n = p.await().asInstanceOf[Int];
      return n == PingPongConfig.N
    }
    
    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      Actor.awaitQuiescence()
    }
  }
  
  private class PingActor(count: Int, pong: PongActor) extends Actor {
    private var pingsLeft: Int = count
    
    override def receive(msg: EventualMessage) {
      msg match {
        case _: StartMessage =>
          pong.send(new PingMessage(pong, this))
          pingsLeft = pingsLeft - 1
        case _: PingMessage =>
          pong.send(new PingMessage(pong, this))
          pingsLeft = pingsLeft - 1
        case _: PongMessage =>
          if (pingsLeft > 0) {
            this.send(new PingMessage(this, this))
          } else {
            pong.send(new StopMessage(pong))
          }
      }
    }
  }
  
  private class PongActor(resolver: Resolver) extends Actor {
    private var pongCount: Int = 0
    
    override def receive(msg: EventualMessage) {
      msg match {
        case message: PingMessage =>
          message.getSender().send(new PongMessage(message.getSender()))
          pongCount = pongCount + 1
        case _: StopMessage =>
          resolver.resolve(pongCount)
      }
    }
  }

  private class StartMessage(target: Actor) extends DirectMessage(target) { }
  
  private class PingMessage(target: Actor, sender: Actor) extends DirectMessage(target) {
    def getSender() : Actor = {
      return sender
    }
  }
  
  private class PongMessage(target: Actor) extends DirectMessage(target) { }
  
  private class StopMessage(target: Actor) extends DirectMessage(target) { }
}