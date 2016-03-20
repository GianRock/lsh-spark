package com.lendap.spark.lsh

/**
  * Created by maruf on 09/08/15.
  */

import com.lendap.spark.lsh.partitioners.BucketPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
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
  /**
    * this function applies  a list of hashfunctions on a sparsevector in input
    * each hashfunctions consiste of one Vector of [-1,1] and will give one boolean as result
    * so if we have k-functions we will hava a signature made of k-bits
    * this signature will be splitted across b-bands
    * so we will have a list of b elements each of which consist of
    * a pair (idBand,signature)
    * an return a list made of  pairs (string
    *
    * @param data
    * @param hashFunctions
    * @return
    */
  private def hashVector(data: SparseVector, hashFunctions: Broadcast[IndexedSeq[(Int,Hasher)]]): List[(Int, String)] = {
    hashFunctions.value.map(a => (a._1 % numHashTables, a._2.hash(data)))
      .groupBy(_._1)
      .map(x => (x._1, x._2.map(_._2).mkString(""))).toList
  }
  def run(sc: SparkContext) : LSHModel = {


    val hashFunctions: IndexedSeq[( Int,Hasher)] = (0 until numHashFunc * numHashTables).map(i => (i,Hasher(m)))
    /**
      * sending the collection of hasher in broadcast to the nodes, is much way efficient
      * because the collection is sent only once to each node.
      */
    val broascastHashFunctions: Broadcast[IndexedSeq[( Int,Hasher)]] = sc.broadcast(hashFunctions)



    //val dataRDD = data.cache()
    /*
    partitioning the hashtables through the band number
    improve the aggreagation on the band.
     */
    val hashTables: RDD[((Int, String), Long)] = data.flatMap {
      case (id, sparseVector) =>
        hashVector(sparseVector, broascastHashFunctions).map((_,id))
    }.partitionBy(new BucketPartitioner(numHashTables)).cache()


    new LSHModel(m,numHashFunc,numHashTables,hashFunctions,hashTables)


  }

  def cosine(a: SparseVector, b: SparseVector): Double = {
    val intersection = a.indices.intersect(b.indices)
    val magnitudeA = intersection.map(x => Math.pow(a.apply(x), 2)).sum
    val magnitudeB = intersection.map(x => Math.pow(b.apply(x), 2)).sum
    intersection.map(x => a.apply(x) * b.apply(x)).sum / (Math.sqrt(magnitudeA) * Math.sqrt(magnitudeB))
  }
}
