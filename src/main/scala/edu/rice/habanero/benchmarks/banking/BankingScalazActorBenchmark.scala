package edu.rice.habanero.benchmarks.banking

import edu.rice.habanero.actors.{ScalazActor, ScalazActorState, ScalazPool}
import edu.rice.habanero.benchmarks.banking.BankingConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.concurrent.Promise
import som.Random
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object BankingScalazActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new BankingScalazActorBenchmark)
  }

  private final class BankingScalazActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      BankingConfig.parseArgs(args)
    }

    def printArgInfo() {
      BankingConfig.printArgs()
    }

    def runIteration() : Future[Double] = {
      val p = Promise[Double]

      val master = new Teller(p, BankingConfig.A, BankingConfig.N)
      master.start()
      master.send(StartMessage.ONLY)

      p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val r = Await.result(f, Duration.Inf)
      return BankingConfig.verify(r);
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      ScalazActorState.awaitTermination()
      
      if (lastIteration) {
        ScalazPool.shutdown()
      }
    }
  }

  protected class Teller(completion: Promise[Double], numAccounts: Int, numBankings: Int) extends ScalazActor[AnyRef] {

    private val self = this
    private val accounts = Array.tabulate[Account](numAccounts)((i) => {
      new Account(i)
    })
    private var numCompletedBankings = 0

    private val randomGen = new Random()
    private var transferredAmount = 0.0


    protected override def onPostStart() {
      accounts.foreach(loopAccount => loopAccount.start())
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
            accounts.foreach(loopAccount => loopAccount.send(StopMessage.ONLY))
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
      srcAccount.send(cm)
    }
  }

  protected class Account(id: Int) extends ScalazActor[AnyRef] {
    private var balance: Double = 0.0
    
    private val self = this
    private var lastSender: Teller = null
    private var waitingForReply = false
    private val requests: ArrayBuffer[CreditMessage] = ArrayBuffer()

    private def processCredit(cm: CreditMessage) {
      balance -= cm.amount
      
      val dest = cm.recipient.asInstanceOf[Account]
      dest.send(new DebitMessage(self, cm.amount))
      
      lastSender = cm.sender.asInstanceOf[Teller]
    }
    
    override def process(theMsg: AnyRef) {
      theMsg match {
        case dm: DebitMessage =>
          balance += dm.amount
          val creditor = dm.sender.asInstanceOf[Account]
          creditor.send(new ReplyMessage(dm.amount))

        case cm: CreditMessage =>
          if (waitingForReply) {
            requests.append(cm)
          } else {
            waitingForReply = true
            processCredit(cm)
          }
          
        case rm: ReplyMessage =>
          lastSender.send(new ReplyMessage(rm.amount))
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
