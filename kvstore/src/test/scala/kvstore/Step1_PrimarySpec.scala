package kvstore

import akka.testkit.TestKit
import akka.actor.ActorSystem
import org.scalatest.FunSuiteLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe
import scala.concurrent.duration._
import kvstore.Persistence.{ Persisted, Persist }
import kvstore.Replica.OperationFailed
import kvstore.Replicator.{ Snapshot }
import scala.util.Random
import scala.util.control.NonFatal
import org.scalactic.ConversionCheckedTripleEquals

class Step1_PrimarySpec extends TestKit(ActorSystem("Step1PrimarySpec"))
    with FunSuiteLike
        with BeforeAndAfterAll
    with Matchers
    with ConversionCheckedTripleEquals
    with ImplicitSender
    with Tools {

  override def afterAll(): Unit = {
    system.shutdown()
  }

  import Arbiter._

  test("case1: Primary (in isolation) should properly register itself to the provided Arbiter") {
    val arbiter = TestProbe()
        system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case1-primary")
    
    arbiter.expectMsg(Join)
  }

  test("case2: Primary (in isolation) should react properly to Insert, Remove, Get") {
    val arbiter = TestProbe()
        val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case2-primary")
        val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    client.getAndVerify("k1")
    client.setAcked("k1", "v1")
    client.getAndVerify("k1")
    client.getAndVerify("k2")
    client.setAcked("k2", "v2")
    client.getAndVerify("k2")
    client.removeAcked("k1")
    client.getAndVerify("k1")
  }

  test("Existing replicas in the cluster must not be assigned new replicators") {
   val arbiter = TestProbe()
   val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "kky1-primary")
   val user = session(primary)

   val secondary1 = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "kky1-secondary-1")
   val secondary2 = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "kky1-secondary-2")
   val secReplica = session(secondary1)

   arbiter.expectMsg(Join)
   arbiter.expectMsg(Join)
   arbiter.expectMsg(Join)

   arbiter.send(primary, JoinedPrimary)
   arbiter.send(secondary1, JoinedSecondary)
   arbiter.send(secondary2, JoinedSecondary)
   arbiter.send(primary, Replicas(Set(primary, secondary1, secondary2)))

   val ack1 = user.set("k1", "v1")
   user.waitAck(ack1)
   assert(secReplica.get("k1") === Some("v1"))

   val ack2 = user.set("k2", "v2")
   user.waitAck(ack2)
   assert(secReplica.get("k2") === Some("v2"))

   val ack3 = user.remove("k1")
   user.waitAck(ack3)
   assert(secReplica.get("k1") === None)
    
   arbiter.send(primary, Replicas(Set(primary, secondary1)))
   val ack4 = user.remove("k2")
   user.waitAck(ack4)
   assert(secReplica.get("k2") === None)

}
}
