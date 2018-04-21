package edu.rice.habanero.benchmarks.banking

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.banking.BankingConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.concurrent.Await
import scala.concurrent.duration._
import som.Random
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BankingAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BankingAkkaActorBenchmark)
  }

  private final class BankingAkkaActorBenchmark extends Benchmark {
    var system: ActorSystem = null
    
    def initialize(args: Array[String]) {
      BankingConfig.parseArgs(args)
    }

    def printArgInfo() {
      BankingConfig.printArgs()
    }

    def runIteration() : Future[Double] = {
      val p = Promise[Double]

      system = AkkaActorState.newActorSystem("Banking")

      val master = system.actorOf(Props(new Teller(p, BankingConfig.A, BankingConfig.N)))
      AkkaActorState.startActor(master)
      master ! StartMessage.ONLY

      p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val r = Await.result(f, Duration.Inf)
      return BankingConfig.verify(r);
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  protected class Teller(completion: Promise[Double], numAccounts: Int, numBankings: Int) extends AkkaActor[AnyRef] {

    private val accounts = Array.tabulate[ActorRef](numAccounts)((i) => {
      context.system.actorOf(Props(new Account(i)))
    })
    private var numCompletedBankings = 0

    private val randomGen = new Random()
    private var transferredAmount = 0.0

    protected override def onPostStart() {
      accounts.foreach(loopAccount => AkkaActorState.startActor(loopAccount))
    }

    override def process(theMsg: AnyRef) {
      theMsg match {
        case sm: BankingConfig.StartMessage =>

          var m = 0
          while (m < numBankings) {
            generateWork()
            m += 1
          }

        case rm: BankingConfig.ReplyMessage =>
          transferredAmount += rm.amount

          numCompletedBankings += 1
          if (numCompletedBankings == numBankings) {
            completion.success(transferredAmount)
            accounts.foreach(loopAccount => loopAccount ! StopMessage.ONLY)
            exit()
          }

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }

    def generateWork(): Unit = {
      // src is lower than dest id to ensure there is never a deadlock
      val srcAccountId = randomGen.next((accounts.length / 10) * 8)
      var loopId = randomGen.next(accounts.length - srcAccountId)
      if (loopId == 0) {
        loopId += 1
      }
      val destAccountId = srcAccountId + loopId

      val srcAccount = accounts(srcAccountId)
      val destAccount = accounts(destAccountId)
      val amount = Math.abs(randomGen.nextDouble()) * 1000

      val sender = self
      val cm = new CreditMessage(sender, amount, destAccount)
      srcAccount ! cm
    }
  }  
  
  protected class Account(id: Int) extends AkkaActor[AnyRef] {
    private var balance: Double = 0.0
    
    private var lastSender: ActorRef = null
    private var waitingForReply = false
    private val requests: ArrayBuffer[CreditMessage]  = ArrayBuffer()

    private def processCredit(cm: CreditMessage) {
      balance -= cm.amount
      
      val dest = cm.recipient.asInstanceOf[ActorRef]
      dest ! new DebitMessage(self, cm.amount)
      
      lastSender = cm.sender.asInstanceOf[ActorRef]
    }
    
    override def process(theMsg: AnyRef) {
      theMsg match {
        case dm: DebitMessage =>
          balance += dm.amount
          sender() ! new ReplyMessage(dm.amount)

        case cm: CreditMessage =>
          if (waitingForReply) {
            requests.append(cm)
          } else {
            waitingForReply = true
            processCredit(cm)
          }
          
        case rm: ReplyMessage =>
          lastSender ! new ReplyMessage(rm.amount)
          if (requests.isEmpty) {
            waitingForReply = false
          } else {
            val req = requests.remove(0)
            processCredit(req)
          }
          
        case _: StopMessage =>
          exit()

        case message =>
          val ex = new IllegalArgumentException("Unsupported message: " + message)
          ex.printStackTrace(System.err)
      }
    }
  }

}
