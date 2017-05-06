package com.github.xubo245.gcdss.load

import htsjdk.samtools.util.Log
import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/6.
  */
object LoadSequenceDictionary {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LoadSequenceDictionary").setMaster("local[16]")
    Log.setGlobalLogLevel(Log.LogLevel.ERROR)
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    LoadRecordGroupDictionary
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
    compute(sc,args(0))
  }

  def compute(sc: SparkContext, fqFile: String): Unit = {
    val alignment = sc.loadAlignments(fqFile)
    var SD = alignment.sequences
    println(SD.records.size)
    SD.records.foreach { each =>
      print("name:" + each.name + "\t")
      print("length:" + each.length + "\t")
      print("url:" + each.url + "\t")
      print("md5:" + each.md5 + "\t")
      print("assembly:" + each.assembly + "\t")
      print("species:" + each.species + "\t")
      print("genbank:" + each.genbank + "\t")
      print("refseq:" + each.refseq + "\t")
      print("referenceIndex:" + each.referenceIndex + "\t")
      println()
    }
  }
  def computeVCF(sc: SparkContext, fqFile: String): Unit = {
    val alignment = sc.loadVariants(fqFile)
    var SD = alignment.sequences
    println(SD.records.size)
    SD.records.foreach { each =>
      print("name:" + each.name + "\t")
      print("length:" + each.length + "\t")
      print("url:" + each.url + "\t")
      print("md5:" + each.md5 + "\t")
      print("assembly:" + each.assembly + "\t")
      print("species:" + each.species + "\t")
      print("genbank:" + each.genbank + "\t")
      print("refseq:" + each.refseq + "\t")
      print("referenceIndex:" + each.referenceIndex + "\t")
      println()
    }
  }
}
