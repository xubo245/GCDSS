package com.github.xubo245.gcdss.ETL

import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variant.VariantRDD

/**
  * Created by xubo on 2017/4/6.
  */
object snpAddSequenceDictionary {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("snpAddSequenceDictionary")
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      conf.setMaster("local[16]")
    }
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
    val alignment = sc.loadAlignments(args(0))
    val variants = sc.loadVariants(args(1))

    var variantsChr = variants.rdd.map { each =>
      var name = each.getContigName
      //      if (!(name.equalsIgnoreCase("X") || name.equalsIgnoreCase("Y"))) {
      each.setContigName("chr" + name)
      //      }
      each
    }.repartition(1)
//    VariantRDD(variants.rdd, alignment.sequences, variants.headerLines).saveAsParquet(args(2))
    VariantRDD(variantsChr, alignment.sequences, variants.headerLines).saveAsParquet(args(2))
  }
}
