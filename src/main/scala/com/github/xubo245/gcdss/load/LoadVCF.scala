package com.github.xubo245.gcdss.load

import htsjdk.samtools.util.Log
import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/5.
  */
object LoadVCF {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LoadVCF").setMaster("local[16]")
    Log.setGlobalLogLevel(Log.LogLevel.ERROR)
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
    //    val vcfFile="file\\callVariant\\input\\vcf\\All_20160407L1000000.vcf"
    //    val vcfFile="file\\callVariant\\input\\vcf\\vcfSelect.vcf20170405220837562"
    //    val vcfFile = "file\\callVariant\\input\\vcf\\vcfSelect.vcf20170405221247894"
//    val vcfFile = "hdfs://219.219.220.149:9000/xubo/callVariant/vcf/vcfSelect.vcf20170406035127942"
    val vcfFile=args(0)
    compute(sc,vcfFile)
    sc.stop()
  }
  def compute(sc:SparkContext,vcfFile:String): Unit ={
    val vcfRDD = sc.loadVariants(vcfFile)
    println("vcfRDD.rdd.count():" + vcfRDD.rdd.count())

    vcfRDD.rdd.take(10).foreach(println)
    println("headerLines:")
    vcfRDD.headerLines.foreach(println)
    println("records:")
    vcfRDD.sequences.records.foreach(println)
    var record=vcfRDD.sequences.records
    println(record.size)

    var oneRDD = vcfRDD.rdd.filter { each =>
      each.getAlternateAllele.length == 1
    }
    var twoRDD = vcfRDD.rdd.filter { each =>
      each.getAlternateAllele.length == 2
    }
    var threeRDD = vcfRDD.rdd.filter { each =>
      each.getAlternateAllele.length == 3
    }

    println("******genotype:"+vcfRDD.rdd.count())
    vcfRDD.rdd.take(20).foreach(println)
    println("******oneRDD:"+oneRDD.count())
    println("******twoRDD:"+twoRDD.count())
    twoRDD.take(20).foreach(println)
    println("******threeRDD:"+threeRDD.count())
    threeRDD.take(20).foreach(println)
  }
}
