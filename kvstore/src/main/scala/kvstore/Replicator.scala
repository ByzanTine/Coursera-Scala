package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.actor.ReceiveTimeout

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  context.setReceiveTimeout(200 milliseconds)
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key: String, valueOption: Option[String], id: Long) => {
      val seq = nextSeq
      replica ! Snapshot(key, valueOption, seq)
      acks += seq -> (sender, Replicate(key, valueOption, id))
      
    }
    case ReceiveTimeout => {
      // resend all unacks
      acks map {
        case (seq, (sender, req)) => replica !  Snapshot(req.key, req.valueOption, seq)
      }
    }
    case SnapshotAck(key: String, seq: Long) => {
      println("snapshotAck: by seq: " + seq)
      val pair = acks(seq)
      pair._1 ! Replicated(key, pair._2.id)
      acks -= seq
      
      context.setReceiveTimeout(200 milliseconds)
    }
    case _ =>
  }

}
