package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }
  
  property("twoEle") = forAll { (a: Int, b:Int) =>
    val h = insert(a, empty)
    val h2 = insert(b, h)
    if (a < b) findMin(h2) == a
    else findMin(h2) == b
  }
  property("delete") = forAll { (a: Int) =>
    val h = insert(a, empty)
    val h2 = deleteMin(h)
    isEmpty(h2)
  }
  property("all elements perseverd") = forAll { (l: List[Int]) =>
   
   def listToQueue(l: List[Int]) : H = {
     if (l.isEmpty) empty
     else insert(l.head, listToQueue(l.tail))
   }
   def traverse(h: H): List[Int] = {
     if (isEmpty(h)) Nil
     else findMin(h) :: traverse(deleteMin(h))
   }
   if(l.isEmpty) true
   else traverse(listToQueue(l)).sorted == l.sorted
    
  }
  property("sorted") = forAll { (h: H) =>
    def isSorted(h: H) : Boolean = {
      if (isEmpty(h)) true
      else {
        val min = findMin(h)
        val dh = deleteMin(h)
        if (isEmpty(dh)) true
        else {
          min <= findMin(dh) && isSorted(dh)
        }
      }
    }
    isSorted(h)
    
  }
  property("meld") = forAll { (h1: H, h2: H) =>
    val h1Min = findMin(h1);
    val h2Min = findMin(h2);
    findMin(meld(h1, h2)) == Math.min(h1Min, h2Min)
    
  }
  lazy val genHeap: Gen[H] = 
  for {
   v <- arbitrary[Int]
   h <- oneOf(const(empty), genHeap)
  } yield insert(v, h)
  // oneOf(const(empty), const(insert(1, insert(123, empty))))

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
