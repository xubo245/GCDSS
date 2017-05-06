package com.github.xubo245.gcdss.load

import htsjdk.samtools.util.Log
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/6.
  */
object LoadAlignmentRecord {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LoadAlignmentRecord")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .set("spark.kryo.referenceTracking", "true")
      .set("spark.kryo.registrationRequired", "true")
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      conf.setMaster("local[16]")
    }
    Log.setGlobalLogLevel(Log.LogLevel.ERROR)
    //    LoadRecordGroupDictionary
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
    compute(sc, args(0))
  }

  def compute(sc: SparkContext, fqFile: String): Unit = {
    val alignment = sc.loadAlignments(fqFile)
    var SD = alignment.rdd
    SD.take(10).foreach { each =>
      println(each)
      println(each.getRecordGroupName == null)
      println(each.getRecordGroupSample == null)
    }
    println("*******count:"+SD.count()+"********")
//    SD.foreach { each =>
//      if (each.getRecordGroupName != null) {
//        println(each.getRecordGroupName+"  ==>"+each.getRecordGroupSample)
//      }
//    }

  }

}
