package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor, Cancellable }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import akka.actor.ReceiveTimeout
import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply
  
  case class OperationFailure(key: String, id: Long)
  
  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // expected sequence number 
  var expSeq = 0L
  // map from id to a operation 
  var pendingOperations = Map.empty[Long, PendingOperation]

  class PendingOperation (val requester: ActorRef, 
      val persist: Persist, 
      val scheduler:Cancellable, 
      val replicators:Set[ActorRef],
      val persisted: Boolean)
      {
    
      }
  
  val persistence = context.actorOf(persistenceProps)
  context.setReceiveTimeout(100 milliseconds)
  
  override def preStart() = {
     arbiter ! Join
  }
 

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }
  // this function got side-effect
  def operate(key: String, valueOption: Option[String], id: Long, requester: ActorRef) = {
    // println("opertation on id: " + id)
    if (valueOption.nonEmpty) {
        // insert
        kv += key->valueOption.get
        requester ! OperationAck(id)
    }
    else {
      // remove
      if (kv.contains(key)) {
        kv -= key
         
      }
      requester ! OperationAck(id) 
//      else {
//        requester ! OperationFailed(id)
//      }
    }
    pendingOperations -= id
  }
  
  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key: String, value: String, id: Long) => {
      // // println("key: " + key +" value: " + value + " id: " + id)
      persistence ! Persist(key, Some(value), id)
      secondaries.values foreach {_ ! Replicate(key, Option(value), id)}
      val scheduler = context.system.scheduler.scheduleOnce(Duration(1000, MILLISECONDS))(self ! OperationFailure(key, id))
      // println("size of replicators when insert" + secondaries.values.size)
      pendingOperations += id -> new PendingOperation(sender, Persist(key, Some(value), id), scheduler, secondaries.values.toSet, false)
    }
    case Remove(key: String, id: Long) => {
      persistence ! Persist(key, None, id)
      secondaries.values foreach {_ ! Replicate(key, None, id)}
      val scheduler = context.system.scheduler.scheduleOnce(Duration(1000, MILLISECONDS))(self ! OperationFailure(key, id))
      // println("size of replicators when remove" + secondaries.values.size)
      pendingOperations += id -> new PendingOperation(sender, Persist(key, None, id), scheduler, secondaries.values.toSet, false)
      
        
    }
    case Get(key: String, id: Long) => {
      if (kv.contains(key)) {
        sender ! GetResult(key, Some(kv(key)), id)  
      }
      else {
        sender ! GetResult(key, None, id) 
      }
      
    }
    case Persisted(key: String, id: Long) => {
      // // println("persisted" + key + " sender: " +  sender)
      // println("persisted: id: " + id)
      val reqPersist = pendingOperations(id)
      pendingOperations += id -> new PendingOperation(reqPersist.requester, 
          reqPersist.persist, reqPersist.scheduler, reqPersist.replicators, true)
      val updateReqPersist = pendingOperations(id)
      val requester = updateReqPersist.requester
      val persistMessage = updateReqPersist.persist
      if (updateReqPersist.persisted && updateReqPersist.replicators.isEmpty) {
        reqPersist.scheduler.cancel()
        operate(key, persistMessage.valueOption, id, requester)
      }
      
      
    }
    case Replicated(key: String, id: Long) => {
      if (pendingOperations.contains(id)) {
        // println("replicated: id: " + id)
        val reqPersist = pendingOperations(id)
       
        pendingOperations += id -> new PendingOperation(reqPersist.requester, 
            reqPersist.persist, reqPersist.scheduler, reqPersist.replicators - sender, reqPersist.persisted)
        val updateReqPersist = pendingOperations(id)
        val requester = updateReqPersist.requester
        val persistMessage = updateReqPersist.persist
        
        if (updateReqPersist.persisted && updateReqPersist.replicators.isEmpty) {
          reqPersist.scheduler.cancel()
          operate(key, persistMessage.valueOption, id, requester)
        }
      }
      // else it's probably handling replicas messages
      
    }
    case ReceiveTimeout => {
      pendingOperations filter (_._2.persisted == false) map {
        case (id, pendingOperation) => persistence ! pendingOperation.persist
      }
      
    }
    case OperationFailure(key: String, id: Long) => {
      println("OperationFailure on id: " + id)
      val reqPersist = pendingOperations(id)
      val requester = reqPersist.requester
      requester ! OperationFailed(id)
    }
    
    case Replicas(replicas: Set[ActorRef]) => {
      // kill non-useful replicators
      val actorsToTerminate = secondaries.keys filter (!replicas.contains(_)) map (secondaries(_))
      actorsToTerminate foreach {_ ! PoisonPill}
      // clean up pendingOperations waiting Replicated Message
      // println("actorsToTerminate: " + actorsToTerminate)
      // println("pendingOperations size: " + pendingOperations.size)
      pendingOperations = pendingOperations map {
        case (id, operation) => {
          val newOperation = new PendingOperation(operation.requester, 
          operation.persist, operation.scheduler, 
          operation.replicators filter (!actorsToTerminate.toSet.contains(_)), operation.persisted)
          id->newOperation
        }
      }
      pendingOperations.values filter {p => p.persisted && p.replicators.isEmpty} foreach {
        p => {
          // // println("one thing finished")
          p.scheduler.cancel()
          operate(p.persist.key, p.persist.valueOption, p.persist.id, p.requester)
        }
      }
        
      
      // allocate new replicator 
      val newReplicas = replicas filter (!secondaries.keys.toSet.contains(_))
      val secondariesToAdd = newReplicas filter {_ != self} map {replica_ => (replica_, context.actorOf(Replicator.props(replica_))) } toMap
      
      secondaries = secondaries filter {case pair => replicas.contains(pair._1)}
      // println("secondariesToAdd size: " + secondariesToAdd.size)
      secondaries = secondaries ++ secondariesToAdd
      // send replicate messages
      
      for {
        replicator <- replicas flatMap {secondaries.get(_)}
        pair <- kv
      } yield replicator ! Replicate(pair._1, Option(pair._2), 0L)
      
      
    }
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key: String, id: Long) => {
      if (kv.contains(key)) {
        sender ! GetResult(key, Some(kv(key)), id)  
      }
      else {
        sender ! GetResult(key, None, id) 
      }
      
    }
    case Snapshot(key: String, valueOption: Option[String], seq: Long) => {
      println("snapshot sending by replicators: seq: " + seq)
      if (pendingOperations.contains(seq)) {
        Unit
      }
      else if (seq > expSeq) Unit
      else if (seq < expSeq) {
        println("secondary replica reply with seq problem on seq: " + seq)
        sender ! SnapshotAck(key, seq)
        expSeq = Math.max(expSeq, seq + 1)
      }
      else {
        persistence ! Persist(key, valueOption, seq)
        secondaries += self->sender
        // persists += persistence -> (sender, Persist(key, valueOption, seq))
        val scheduler = context.system.scheduler.scheduleOnce(Duration(1000, MILLISECONDS))(self ! OperationFailure(key, seq))
        pendingOperations += seq -> new PendingOperation(sender, Persist(key, valueOption, seq), scheduler, Set.empty, false)
        if (valueOption.nonEmpty) kv += key->valueOption.get
        else kv -= key
        expSeq += 1
      }
      
      
    }
    case Persisted(key: String, id: Long) => {
      // secondaries(self) ! SnapshotAck(key, id)
      println("secondary replica persisted on id: " + id)
      val seq = id
      val requester = pendingOperations(id).requester
    
      requester ! SnapshotAck(key, seq)
      pendingOperations -= id
    }
    
    case ReceiveTimeout => {
      pendingOperations map {
        case (id, pendingOperation) => persistence ! pendingOperation.persist
      }
      
    }
    case _ =>
  }

}

