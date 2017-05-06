package com.github.xubo245.gcdss.adam.postProcessing

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.util.Random

/**
  * Created by xubo on 2017/4/5.
  */
class AdamSortSuite extends GcdssAlignmentFunSuite {
  test("AdamSort 1") {

    val random = new Random("sortingIndices".hashCode)
    val numReadsToCreate = 10
    val reads = for (i <- 0 until numReadsToCreate) yield {
      val mapped = random.nextBoolean()
      val builder = AlignmentRecord.newBuilder().setReadMapped(mapped)
//      println(builder.getReadMapped)
      if (mapped) {
        val contigName = random.nextInt(numReadsToCreate / 10).toString
        val start = random.nextInt(1000000).toLong
        builder.setContigName(contigName)
          .setStart(start)
          .setEnd(start)
      }
      builder.setReadName((0 until 20).map(i => (random.nextInt(100) + 64)).mkString)
      builder.build()
    }
    val contigNames = reads.filter(_.getReadMapped).map(_.getContigName).toSet
    val sd = new SequenceDictionary(contigNames.toSeq
      .zipWithIndex
      .map(kv => {
        val (name, index) = kv
        SequenceRecord(name, Int.MaxValue, referenceIndex = Some(index))
      }).toVector)
    val rdd = sc.parallelize(reads)
    rdd.foreach(println)
    val sortedReads = AlignmentRecordRDD(rdd, sd, RecordGroupDictionary.empty)
      .sortReadsByReferencePositionAndIndex()
      .rdd
      .collect()
      .zipWithIndex
    println("sort")
    sortedReads.foreach(println)
    val (mapped, unmapped) = sortedReads.partition(_._1.getReadMapped)

    // Make sure that all the unmapped reads are placed at the end
    assert(unmapped.forall(p => p._2 > mapped.takeRight(1)(0)._2))

    def toIndex(r: AlignmentRecord): Int = {
      sd(r.getContigName).get.referenceIndex.get
    }

    // Make sure that we appropriately sorted the reads
    import scala.math.Ordering._

    val expectedSortedReads = mapped.map(kv => {
      val (r, idx) = kv
      val start: Long = r.getStart
      ((toIndex(r), start), (r, idx))
    }).sortBy(_._1)
      .map(_._2)
    assert(expectedSortedReads === mapped)
  }
}