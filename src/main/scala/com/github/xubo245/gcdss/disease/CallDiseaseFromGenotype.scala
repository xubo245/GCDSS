package com.github.xubo245.gcdss.disease

import java.text.SimpleDateFormat
import java.util.Date

import com.github.xubo245.gcdss.utils.Constants
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/9.
  */
object CallDiseaseFromGenotype {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    var genotypeFile = args(0)
    val vcf2omimSimpleFile = args(1)
    var out = args(2)
    var appArgs = "genotypeFile:" + genotypeFile + "\tout:" + out
    val conf = new SparkConf().setAppName("CallDiseaseFromGenotype:" + appArgs)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .set("spark.kryo.referenceTracking", "true")
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      conf.setMaster("local[16]")
    }
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)

    Constants.debug = false
    //    var vcfFile = "file/callDisease/input/vcf/small.vcf"
    //    val vcfFile = "file\\callVariant\\output\\DiscoverVariant20170409211412241.adam"
    //    var output = "file/callDisease/output/vcf2omim/test1"
    //    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimAll.txt"
    //    val rdd = sc.loadVcf(vcfFile, sd)
    //    rdd.map(_.variant.variant).foreach(println)
    val rdd = sc.loadGenotypes(genotypeFile)
    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    val vcf2Omim = new CallDisease(rdd.toVariantContextRDD)
    val returnRDD = vcf2Omim.runComplex(sc, vcf2omimSimpleFile)

    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output1 = out + "/ComplexT" + iString

    val saveRDD = returnRDD.map { each =>
      val str1 = each._1.split(Array(',', '(', ')'))
      val str = str1(1) + '|' + str1(2) + '|' + str1(3) + '|' + str1(4) + '|' + each._2 + '|' +
        each._3 + '|' + each._4 + '|' + each._5 + '|' + each._6 + '|' + each._7 + '|' +
        each._8 + '|' + each._9 + '|' + each._10 + '|' + each._11 + '|' + each._12 + '|' +
        each._13 + '|' + each._14 + '|' + each._15
      str
    }

    println("*************count:"+saveRDD.count())
//    saveRDD.foreach(println)

    saveRDD.repartition(1).saveAsTextFile(output1)

    sc.stop
    val stopTime = System.currentTimeMillis()

    println(appArgs + "\ttime:\t" + (stopTime - startTime) / 1000.0 + "\t")
  }
}
