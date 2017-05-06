package com.github.xubo245.gcdss.adam.postProcessing

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.cli.Transform
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/6.
  */
object GcdssTransform {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("GcdssTransform")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .set("spark.kryo.referenceTracking", "true")
      .set("spark.kryo.registrationRequired", "true")
    //      .setMaster("local[16]")
    //    if (System.getProperties.getProperty("os.name").contains("Windows")) {
    //      conf.setMaster("local[16]")
    //    }
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
    val fqFile = args(0)
    val out = args(1)
    val vcfFile = args(2)
    compute(sc, fqFile, out, vcfFile)
    sc.stop
    val stopTime = System.currentTimeMillis()
    var appArgs = "fqFile:" + fqFile + "\tout:" + out + "\tvcfFile:" + vcfFile
    println(appArgs + "\ttime:\t" + (stopTime - startTime) / 1000.0 + "\t")
  }

  def compute(sc: SparkContext, fqFile: String, out: String, vcfFile: String): Unit = {
    //    Transform(Array("-single", "-sort_reads", "-sort_lexicographically", "-recalibrate_base_qualities", "-known_snps", vcfFile, fqFile, out)).run(sc)
    //    Transform(Array("-single", "-sort_reads", "-sort_lexicographically", "-realign_indels", "-mark_duplicate_reads", "-recalibrate_base_qualities", "-known_snps", vcfFile, fqFile, out)).run(sc)
    Transform(Array("-sort_reads", "-sort_lexicographically", "-realign_indels", "-mark_duplicate_reads", "-recalibrate_base_qualities", "-known_snps", vcfFile, fqFile, out)).run(sc)

  }
}
