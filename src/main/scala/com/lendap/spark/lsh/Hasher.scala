package com.lendap.spark.lsh

import org.apache.spark.mllib.linalg.SparseVector
import scala.collection.mutable.ArrayBuffer
import scala.util.Random




/**
  * Simple hashing function implements random hyperplane based hash functions described in
  * http://www.cs.princeton.edu/courses/archive/spring04/cos598B/bib/CharikarEstim.pdf
  * r is a random vector. Hash function h_r(u) operates as follows:
  * if r.u < 0 //dot product of two vectors
  *    h_r(u) = 0
  *  else
  *    h_r(u) = 1

  this class use a much compact rappresentaion of data:
  instead of an array of double whose elements are [-1,1],i've adopted
  a vector of Boolean -1=>false,1=>true
  **/
class Hasher(val r: Vector[Boolean]) extends Serializable {
  /** hash SparseVector v with random vector r */
  def hash(u : SparseVector) : Int = {
    val rVec: Array[Boolean] = u.indices.map(i => r(i))
    val hashVal = (rVec zip u.values).map(_tuple => if(_tuple._1) _tuple._2 else -_tuple._2).sum
    if (hashVal > 0) 1 else 0
  }

}

object Hasher extends Serializable{

  def fromString(stringSerialized: String): Vector[Boolean] ={
    //println(stringSerialized)
    val vector=stringSerialized.par.map(x=>if(x=='0') false else true).toVector
    //println(vector)
    vector
  }

  def apply(stringSerialized:String)={
    new Hasher(fromString(stringSerialized))

  }

  /** create a new instance providing size of the random vector Array [Double] */
  def apply (size: Int, seed: Long = System.nanoTime) = new Hasher(r(size, seed))

  /** create a random vector whose whose components are -1 and +1 */
  def r(size: Int, seed: Long): Vector[Boolean] = {
    val rnd = new Random(seed)
    (0 until  size).map{
      _=> if (rnd.nextGaussian() < 0) false else true

    }.toVector
  }





}