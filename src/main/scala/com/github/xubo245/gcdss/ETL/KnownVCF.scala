package com.github.xubo245.gcdss.ETL

import java.text.SimpleDateFormat
import java.util.Date

import htsjdk.samtools.ValidationStringency
import htsjdk.samtools.util.Log
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variant.VariantRDD

/**
  * Created by xubo on 2017/4/5.
  */

object KnownVCF {
  def main(args: Array[String]) {
    Log.setGlobalLogLevel(Log.LogLevel.ERROR)
    val conf = new SparkConf().setAppName("KnownVCF")
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.serializer", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    //      .set("spark.kryo.serializer", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    //      .set("spark.kryo.referenceTracking", "true")
    //      .set("spark.kryo.registrationRequired", "true")
    //      .set("spark.driver.allowMultipleContexts", "true")

    //    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    //    "spark.kryo.registrator" -> "org.bdgenomics.adam.serialization.ADAMKryoRegistrator",
    //    "spark.kryo.referenceTracking" -> "true",
    //    "spark.kryo.registrationRequired" -> "true"

    //      conf.setMaster("local[1]")
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      conf.setMaster("local[16]")
    }
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
    //    val vcfFile = "file\\callVariant\\input\\vcf\\All_20160407L1000000.vcf"
    //    val vcfFile = "hdfs://219.219.220.149:9000/xubo/callVariant/vcf/All_20160407.vcf"
    //    val vcf2omimFile = "file\\callVariant\\input\\vcf\\vcf2omimAllResult.txt"
    val vcfFile = args(0)
    val vcf2omimFile = args(1)
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    compute(vcfFile, vcf2omimFile, sc, args(2) + iString)

    sc.stop()
  }

  def compute(vcfFile: String, vcf2omimFile: String, sc: SparkContext, out: String): Unit = {
    val vcfRDD = sc.loadVariants(vcfFile)
    //    println(vcfRDD.rdd.count())
    //    vcfRDD.rdd.take(10).foreach(println)

    val vcf2omimRDD = sc.textFile(vcf2omimFile)
    //    var snpID = vcf2omimRDD.map { each =>
    //      var lineArr = each.split('|')
    //      lineArr(6)
    //    }.collect().distinct
    val vcf2omimArr = vcf2omimRDD.collect()
    var snpID = vcf2omimRDD.map { each =>
      var lineArr = each.split('|')
      lineArr(6)
    }.collect().distinct

    vcf2omimArr.length

    //    snpID.take(10).foreach(println)

    //    vcfRDD.rdd.take(10).map(_.getNames).foreach(println)
    //println(snpID.length)
    var knownVCF = vcfRDD.rdd.filter { each =>
      var namesList = each.getNames
      var flag = false
      for (i <- 0 until snpID.length) {
        //name.size()
        if (flag == false) {
          val namesArr = namesList.toArray()
          for (j <- 0 until namesArr.length) {
            var name = namesArr(j)
            if (snpID(i) == name && flag == false) {
              flag = true
              true
            }
          }
        }
      }
      if (flag == true) {
        true
      } else {
        false
      }
    }
    println("knownVCF")
    knownVCF.rdd.context.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.serializer", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .set("spark.kryo.referenceTracking", "true")
      .set("spark.kryo.registrationRequired", "true")
    println("count:" + knownVCF.count())
    //    knownVCF.take(10).foreach(println)
    val varRDD = VariantRDD(knownVCF, vcfRDD.sequences, vcfRDD.headerLines)
    varRDD.saveAsParquet(out)
    //    varRCDD.saveAsVcf(out,asSingleFile=false,stringency=ValidationStringency.LENIENT)
  }
}
