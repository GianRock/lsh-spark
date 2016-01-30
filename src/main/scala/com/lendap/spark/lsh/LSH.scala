package com.lendap.spark.lsh

/**
  * Created by maruf on 09/08/15.
  */

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._

/** Build LSH model with data RDD. Hash each vector number of hashTable times and stores in a bucket.
  *
  * @param data          RDD of sparse vectors with vector Ids. RDD(vec_id, SparseVector)
  * @param m             max number of possible elements in a vector
  * @param numHashFunc   number of hash functions
  * @param numHashTables number of hashTables.
  *
  * */
class LSH(data: RDD[(Long, SparseVector)] = null, m: Int = 0, numHashFunc: Int = 4, numHashTables: Int = 4) extends Serializable {

  def run(): LSHModel = {

    val hashFunctions = (0 until numHashFunc * numHashTables).map(i => (Hasher(m),i))

    def hashVector(data: SparseVector): List[(Int, String)] =
      hashFunctions.map(a => (a._2 % numHashTables, a._1.hash(data)))
        .groupBy(_._1)
        .map(x => (x._1, x._2.map(_._2).mkString(""))).toList

    //create a new model object
    val dataRDD = data.cache()

    //compute hash keys for each vector
    // - hash each vector numHashFunc times
    // - concat each hash value to create a hash key
    // - position hashTable id hash keys and vector id into a new RDD.
    // - creates RDD of ((hashTable#, hash_key), vec_id) tuples.
//    model.hashTables = dataRDD
//      .map(v => (model.hashFunctions.map(h => (h._1.hash(v._2), h._2 % numHashTables)), v._1))
//      .map(x => x._1.map(a => ((a._2, x._2), a._1)))
//      .flatMap(a => a).groupByKey()
//      .map(x => ((x._1._1, x._2.mkString("")), x._1._2)).cache()

    val hashTables = data.flatMap {
      case (id, sparseVector) =>
        hashVector(sparseVector).map((_,id))
    }.cache()

    val model = new LSHModel(m, numHashFunc, numHashTables, hashFunctions, hashTables)
    model
  }

  def cosine(a: SparseVector, b: SparseVector): Double = {
    val intersection = a.indices.intersect(b.indices)
    val magnitudeA = intersection.map(x => Math.pow(a.apply(x), 2)).sum
    val magnitudeB = intersection.map(x => Math.pow(b.apply(x), 2)).sum
    intersection.map(x => a.apply(x) * b.apply(x)).sum / (Math.sqrt(magnitudeA) * Math.sqrt(magnitudeB))
  }
}
