package com.github.xubo245.gcdss.ETL

import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{RecordGroupDictionary, RecordGroup}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.variant.VariantRDD

/**
  * Created by xubo on 2017/4/6.
  */
object SAMAddRecordGroupDictionary {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SAMAddRecordGroupDictionary")
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      conf.setMaster("local[16]")
    }
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
    val alignment = sc.loadAlignments(args(0))
    //make read group dictionary
    //    val readGroup = new RecordGroup("sample", "sample")
    var recordGroupName = "machine foo"
    var recordGroupSample = "sample"
    val readGroup = new RecordGroup(recordGroupSample, recordGroupName, library = Some("library bar"))
    val readGroups = new RecordGroupDictionary(Seq(readGroup))
    var RDD = alignment.rdd.map { each =>
      each.setRecordGroupName(recordGroupName)
      each.setRecordGroupSample(recordGroupSample)
      each
    }
    //      VariantRDD(variantsChr, alignment.sequences, variants.headerLines).saveAsParquet(args(2))
    AlignmentRecordRDD(RDD, alignment.sequences, readGroups).saveAsParquet(args(1))
    sc.stop()
  }
}
