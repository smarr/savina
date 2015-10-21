package edu.rice.habanero.benchmarks.uct

import akka.actor.{ActorRef, ActorSystem, Props}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.uct.UctConfig._
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import som.Random


/**
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object UctAkkaActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new UctAkkaActorBenchmark)
  }

  private final class UctAkkaActorBenchmark extends Benchmark {
    var system: ActorSystem = null

    def initialize(args: Array[String]) {
      UctConfig.parseArgs(args)
    }

    def printArgInfo() {
      UctConfig.printArgs()
    }

    def runIteration() : Future[Int] = {
      system = AkkaActorState.newActorSystem("UCT")
      val p = Promise[Int]

      val rootActor = system.actorOf(Props(new RootActor(p)))
      AkkaActorState.startActor(rootActor)
      rootActor ! new GenerateTreeMessage()

      return p.future
    }

    override def runAndVerify() : Boolean = {
      val f = runIteration()
      val n = Await.result(f, Duration.Inf)
      return n == UctConfig.MAX_NODES - UctConfig.BINOMIAL_PARAM
    }
    
    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
      AkkaActorState.awaitTermination(system)
    }
  }

  /**
   * @author xinghuizhao
   * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
   */
  protected class RootActor(completion: Promise[Int]) extends AkkaActor[AnyRef] {

    private final val ran: Random = new Random()
    private var height: Int = 1
    private var size: Int = 1
    private final val children = new Array[ActorRef](UctConfig.BINOMIAL_PARAM)
    private final val hasGrantChildren = new Array[Boolean](UctConfig.BINOMIAL_PARAM)
    private var traversedChildren: Int = 0
    private var subtreeSize: Int = 0
    private var startedTraversal: Boolean = false

    override def process(theMsg: AnyRef) {
      theMsg match {
        case _: UctConfig.GenerateTreeMessage =>
          generateTree()
        case grantMessage: UctConfig.UpdateGrantMessage =>
          updateGrant(grantMessage.childId)
        case booleanMessage: UctConfig.ShouldGenerateChildrenMessage =>
          val sender: ActorRef = booleanMessage.sender.asInstanceOf[ActorRef]
          checkGenerateChildrenRequest(sender, booleanMessage.childHeight)
        case _: UctConfig.PrintInfoMessage =>
          printInfo()
        case tdMsg: UctConfig.TraversedMessage =>
          traversed(tdMsg.treeSize)
        case _ =>
      }
    }

    /**
     * This message is called externally to create the BINOMIAL_PARAM tree
     */
    def generateTree() {
      height += 1
      val computationSize: Int = getNextNormal(UctConfig.AVG_COMP_SIZE, UctConfig.STDEV_COMP_SIZE)

      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {
        hasGrantChildren(i) = false
        children(i) = NodeActor.createNodeActor(context.system, self, self, height, size + i, computationSize)
        i += 1
      }
      size += UctConfig.BINOMIAL_PARAM

      var j: Int = 0
      while (j < UctConfig.BINOMIAL_PARAM) {
        children(j) ! new TryGenerateChildrenMessage()
        j += 1
      }
    }

    /**
     * This message is called by a child node before generating children;
     * the child may generate children only if this message returns true
     *
     * @param childName The child name
     * @param childHeight The height of the child in the tree
     */
    def checkGenerateChildrenRequest(childName: ActorRef, childHeight: Int) {
      if (size + UctConfig.BINOMIAL_PARAM <= UctConfig.MAX_NODES) {
        val moreChildren: Boolean = ran.nextBoolean
        if (moreChildren) {
          val childComp: Int = getNextNormal(UctConfig.AVG_COMP_SIZE, UctConfig.STDEV_COMP_SIZE)
          val randomInt: Int = ran.next(100)
          
          childName ! new UctConfig.GenerateChildrenMessage(size, childComp)
          
          size += UctConfig.BINOMIAL_PARAM
          if (childHeight + 1 > height) {
            height = childHeight + 1
          }
        }
        else if (childHeight > height) {
          height = childHeight
        }
      } else if (!startedTraversal) {
        startedTraversal = true
        traverse()
      }
    }

    /**
     * This method is called by getBoolean in order to generate computation times for actors, which
     * follows a normal distribution with mean value and a std value
     */
    def getNextNormal(pMean: Int, pDev: Int): Int = {
      var result: Int = 0
      while (result <= 0) {
        val tempDouble: Double = ran.nextGaussian * pDev + pMean
        result = Math.round(tempDouble).asInstanceOf[Int]
      }
      result
    }

    /**
     * This message is called by a child node to indicate that it has children
     */
    def updateGrant(childId: Int) {
      hasGrantChildren(childId) = true
    }

    /**
     * This is the method for traversing the tree
     */
    def traverse() {
      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {
        children(i) ! new TraverseMessage()
        i += 1
      }
    }
    
    def traversed(treeSize: Int) {
      traversedChildren += 1
      subtreeSize += treeSize
      if (traversedChildren == UctConfig.BINOMIAL_PARAM) {
        completion.success(subtreeSize)  // height * size
        exit()
      }
    }

    def printInfo() {
      System.out.println("0 0 children starts 1")
      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {
        children(i) ! new PrintInfoMessage()
        i += 1
      }
    }
  }

  /**
   * @author xinghuizhao
   * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
   */
  protected object NodeActor {
    def createNodeActor(system: ActorSystem, parent: ActorRef, root: ActorRef, height: Int, id: Int, comp: Int): ActorRef = {
      val nodeActor = system.actorOf(Props(new NodeActor(parent, root, height, id, comp)))
      AkkaActorState.startActor(nodeActor)
      nodeActor
    }
  }

  protected class NodeActor(myParent: ActorRef, myRoot: ActorRef, myHeight: Int, myId: Int, myCompSize: Int) extends AkkaActor[AnyRef] {

    private var hasChildren: Boolean = false
    private var traversedChildren: Int = 0
    private var subtreeSize: Int = 0
    private final val children = new Array[ActorRef](UctConfig.BINOMIAL_PARAM)
    private final val hasGrantChildren = new Array[Boolean](UctConfig.BINOMIAL_PARAM)
    private final val busyLoopRan = new Random()

    override def process(theMsg: AnyRef) {
      theMsg match {
        case _: UctConfig.TryGenerateChildrenMessage =>
          tryGenerateChildren()
        case childrenMessage: UctConfig.GenerateChildrenMessage =>
          generateChildren(childrenMessage.currentId, childrenMessage.compSize)
        case grantMessage: UctConfig.UpdateGrantMessage =>
          updateGrant(grantMessage.childId)
        case _: UctConfig.TraverseMessage =>
          traverse()
        case tdMsg: UctConfig.TraversedMessage =>
          traversed(tdMsg.treeSize)
        case _: UctConfig.PrintInfoMessage =>
          printInfo()
        case _: UctConfig.GetIdMessage =>
          getId
        case _ =>
      }
    }

    /**
     * This message is called by parent node, try to generate children of this node.
     * If the "getBoolean" message returns true, the node is allowed to generate BINOMIAL_PARAM children
     */
    def tryGenerateChildren() {
      UctConfig.loop(UctConfig.AVG_COMP_SIZE / 50, busyLoopRan)
      myRoot ! new UctConfig.ShouldGenerateChildrenMessage(self, myHeight)
    }

    def generateChildren(currentId: Int, compSize: Int) {
      val myArrayId: Int = myId % UctConfig.BINOMIAL_PARAM
      myParent ! new UctConfig.UpdateGrantMessage(myArrayId)
      val childrenHeight: Int = myHeight + 1
      val idValue: Int = currentId

      var i: Int = 0
      while (i < UctConfig.BINOMIAL_PARAM) {
        children(i) = NodeActor.createNodeActor(context.system, self, myRoot, childrenHeight, idValue + i, compSize)
        i += 1
      }

      hasChildren = true

      var j: Int = 0
      while (j < UctConfig.BINOMIAL_PARAM) {
        children(j) ! new TryGenerateChildrenMessage()
        j += 1
      }
    }

    /**
     * This message is called by a child node to indicate that it has children
     */
    def updateGrant(childId: Int) {
      hasGrantChildren(childId) = true
    }

    /**
     * This message is called by parent while doing a traverse
     */
    def traverse() {
      traversedChildren = 0
      UctConfig.loop(myCompSize, busyLoopRan)
      if (hasChildren) {
        var i: Int = 0
        while (i < UctConfig.BINOMIAL_PARAM) {
          children(i) ! new TraverseMessage()
          i += 1
        }
      } else {
        myParent ! new TraversedMessage(1)
        exit()
      }
    }
    
    def traversed(treeSize: Int) {
      subtreeSize += treeSize
      traversedChildren += 1
      if (traversedChildren == UctConfig.BINOMIAL_PARAM) {
        myParent ! new TraversedMessage(subtreeSize + 1)
        exit()
      }
    }

    def printInfo() {
      if (hasChildren) {
        System.out.println(myId + " " + myCompSize + "  children starts ")

        var i: Int = 0
        while (i < UctConfig.BINOMIAL_PARAM) {
          children(i) ! new PrintInfoMessage()
          i += 1
        }
      } else {
        System.out.println(myId + " " + myCompSize)
      }
    }

    def getId: Int = {
      myId
    }
  }
}
