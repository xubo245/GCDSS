package com.github.xubo245.gcdss.adam.postProcessing

import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.cli.Transform
import org.bdgenomics.adam.rdd.ADAMContext

/**
  * Created by xubo on 2017/4/6.
  */
object ReadPostProcessing {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    val typeStr = args(0)
    val fqFile = args(1)
    val out = args(2)
    var vcfFile = new String
    if (args.length > 3) {
      vcfFile = args(3)
    }
    var appArgs = "typeStr:"+typeStr+"\tfqFile:" + fqFile + "\tout:" + out + "\tvcfFile:" + vcfFile

    val conf = new SparkConf().setAppName("ReadPostProcessing:" + appArgs)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .set("spark.kryo.referenceTracking", "true")
      .set("spark.kryo.registrationRequired", "true")
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      conf.setMaster("local[16]")
    }
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)

    if (typeStr.equalsIgnoreCase("sort")) {
      sort(sc, fqFile, out)
    }
    if (typeStr.equalsIgnoreCase("BQSR")) {
      if (args.length > 3) {
        BQSR(sc, fqFile, out, vcfFile)
      } else {
        BQSR(sc, fqFile, out)
      }
    }
    if (typeStr.equalsIgnoreCase("realignIndel")) {
      realignIndel(sc, fqFile, out)
    }
    if (typeStr.equalsIgnoreCase("markDuplicate")) {
      markDuplicate(sc, fqFile, out)
    }


    //    compute(sc, fqFile, out, vcfFile)
    sc.stop
    val stopTime = System.currentTimeMillis()

    println(appArgs + "\ttime:\t" + (stopTime - startTime) / 1000.0 + "\t")
  }

  def compute(sc: SparkContext, fqFile: String, out: String, vcfFile: String): Unit = {
    Transform(Array("-single", "-sort_reads", "-sort_lexicographically", "-recalibrate_base_qualities", "-known_snps", vcfFile, fqFile, out)).run(sc)
  }

  def sort(sc: SparkContext, fqFile: String, out: String): Unit = {
    Transform(Array("-single", "-sort_reads", "-sort_lexicographically", fqFile, out)).run(sc)
  }

  def BQSR(sc: SparkContext, fqFile: String, out: String): Unit = {
    Transform(Array("-single", "-recalibrate_base_qualities", fqFile, out)).run(sc)
  }

  def BQSR(sc: SparkContext, fqFile: String, out: String, vcfFile: String): Unit = {
    Transform(Array("-single", "-recalibrate_base_qualities", "-known_snps", vcfFile, fqFile, out)).run(sc)
  }

  def realignIndel(sc: SparkContext, fqFile: String, out: String): Unit = {
    Transform(Array("-single", "-realign_indels", fqFile, out)).run(sc)
  }

  def markDuplicate(sc: SparkContext, fqFile: String, out: String): Unit = {
    Transform(Array("-single", "-mark_duplicate_reads", fqFile, out)).run(sc)
  }
}
