package edu.rice.habanero.benchmarks.barber

import som.Random
import java.util.concurrent.atomic.AtomicLong

import edu.rice.habanero.actors.{ScalazActor, ScalazActorState, ScalazPool}
import edu.rice.habanero.benchmarks.barber.SleepingBarberConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * source: https://code.google.com/p/gparallelizer/wiki/ActorsExamples
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object SleepingBarberScalazActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new SleepingBarberScalazActorBenchmark)
  }

  private final class SleepingBarberScalazActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      SleepingBarberConfig.parseArgs(args)
    }

    def printArgInfo() {
      SleepingBarberConfig.printArgs()
    }

    def runIteration() : Future[Integer] = {
      val p = Promise[Integer]

      val barber = new BarberActor(p)
      val room = new WaitingRoomActor(SleepingBarberConfig.W, barber)
      val factoryActor = new CustomerFactoryActor(SleepingBarberConfig.N, room)

      barber.start()
      room.start()
      factoryActor.start()

      factoryActor.send(Start.ONLY)

      p.future
    }
    
    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val r = Await.result(f, Duration.Inf)
      return SleepingBarberConfig.verify(r)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      ScalazActorState.awaitTermination()
      
      if (lastIteration) {
        ScalazPool.shutdown()
      }
    }
  }


  private case class Enter(customer: ScalazActor[AnyRef], room: ScalazActor[AnyRef])

  private case class Returned(customer: ScalazActor[AnyRef])


  private class WaitingRoomActor(capacity: Int, barber: BarberActor) extends ScalazActor[AnyRef] {

    private val self = this
    private val waitingCustomers = new ListBuffer[ScalazActor[AnyRef]]()
    private var barberAsleep = true

    override def process(msg: AnyRef) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          if (waitingCustomers.size == capacity) {

            customer.send(Full.ONLY)

          } else {

            waitingCustomers.append(customer)
            if (barberAsleep) {

              barberAsleep = false
              self.send(Next.ONLY)

            } else {

              customer.send(Wait.ONLY)
            }
          }

        case message: Next =>

          if (waitingCustomers.size > 0) {

            val customer = waitingCustomers.remove(0)
            barber.send(new Enter(customer, self))

          } else {

            barber.send(Wait.ONLY)
            barberAsleep = true

          }

        case message: Exit =>

          barber.send(Exit.ONLY)
          exit()

      }
    }
  }

  private class BarberActor(completion: Promise[Integer]) extends ScalazActor[AnyRef] {

    private val random = new Random()

    override def process(msg: AnyRef) {
      msg match {
        case message: Enter =>

          val customer = message.customer
          val room = message.room

          customer.send(Start.ONLY)
          // println("Barber: Processing customer " + customer)
          SleepingBarberConfig.busyWait(random, random.next(SleepingBarberConfig.AHR) + 10)
          customer.send(Done.ONLY)
          room.send(Next.ONLY)

        case message: Wait =>

        // println("Barber: No customers. Going to have a sleep")

        case message: Exit =>
          completion.success(random.next())
          exit()

      }
    }
  }

  private class CustomerFactoryActor(haircuts: Int, room: WaitingRoomActor) extends ScalazActor[AnyRef] {

    private val self = this
    private val random = new Random()
    private var numHairCutsSoFar = 0
    private var idGenerator = 0

    override def process(msg: AnyRef) {
      msg match {
        case message: Start =>

          var i = 0
          while (i < haircuts) {
            sendCustomerToRoom()
            SleepingBarberConfig.busyWait(random, random.next(SleepingBarberConfig.APR) + 10)
            i += 1
          }

        case message: Returned =>
          idGenerator += 1
          sendCustomerToRoom(message.customer)

        case message: Done =>

          numHairCutsSoFar += 1
          if (numHairCutsSoFar == haircuts) {
            room.send(Exit.ONLY)
            exit()
          }
      }
    }

    private def sendCustomerToRoom() {
      val customer = new CustomerActor(idGenerator, self)
      customer.start()

      sendCustomerToRoom(customer)
    }

    private def sendCustomerToRoom(customer: ScalazActor[AnyRef]) {
      val enterMessage = new Enter(customer, room)
      room.send(enterMessage)
    }
  }

  private class CustomerActor(val id: Long, factoryActor: CustomerFactoryActor) extends ScalazActor[AnyRef] {

    private val self = this

    override def process(msg: AnyRef) {
      msg match {
        case message: Full =>

          // println("Customer-" + id + " The waiting room is full. I am leaving.")
          factoryActor.send(new Returned(self))

        case message: Wait =>

        // println("Customer-" + id + " I will wait.")

        case message: Start =>

        // println("Customer-" + id + " I am now being served.")

        case message: Done =>

          //  println("Customer-" + id + " I have been served.")
          factoryActor.send(Done.ONLY)
          exit()
      }
    }
  }

}
