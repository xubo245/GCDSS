package com.github.xubo245.gcdss.test

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.algorithms.consensus.ConsensusGenerator
import org.bdgenomics.adam.models.SnpTable
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.AlignmentRecord

/**
  * Created by xubo on 2017/4/5.
  */
object ReadPostProcessingTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AvocadoSuite").setMaster("local[16]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
    val fqFile = "file/callVariant/input/sam/artificial.realigned.sam"
    //    val fqFile = "file/callVariant/input/sam/artificial.sam"
    val alignment = sc.loadAlignments(fqFile)
    //    alignment.rdd.take(10).foreach(println)
    alignment.rdd.foreach(println)
    val sortAlignment = alignment.sortReadsByReferencePositionAndIndex()
    println("sort:")
    sortAlignment.rdd.foreach(println)
    val realignALignment = sortAlignment.realignIndels(consensusModel = ConsensusGenerator.fromReads)
    println("realignIndels:")
    realignALignment.rdd.foreach(println)

    val MD=realignALignment.markDuplicates()
    MD.rdd.foreach(println)

    println("BQSRRDD:")
    val vcfFile="file/callVariant/input/vcf/vcfSelect.adam"
    val variants = sc.loadVariants(vcfFile)
    //    val variants: RDD[RichVariant] = sc.loadVariants(vcfFile).rdd.map(new RichVariant(_))
    val snps = sc.broadcast(SnpTable(variants))
//    val BQSRRDD=alignment.recalibateBaseQualities(snps)
//    BQSRRDD.rdd.take(10).foreach(println)
    sc.stop()

  }
}
