package com.github.xubo245.gcdss.load

import htsjdk.samtools.util.Log
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/6.
  */
object LoadRecordGroupDictionary {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LoadRecordGroupDictionary").setMaster("local[16]")
    Log.setGlobalLogLevel(Log.LogLevel.ERROR)
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    LoadRecordGroupDictionary
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
    compute(sc,args(0))
    sc.stop
  }

  def compute(sc: SparkContext, fqFile: String): Unit = {
    val alignment = sc.loadAlignments(fqFile)
    var SD = alignment.recordGroups.recordGroups
    println(SD.size)
    SD.foreach { each =>
      println(each)
    }
  }

}
