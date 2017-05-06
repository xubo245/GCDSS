package com.github.xubo245.gcdss.disease

import java.text.SimpleDateFormat
import java.util.Date

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{SequenceDictionary, VariantContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.scalatest.FunSuite

/**
  * Created by xubo on 2016/6/9.
  */
class vcf2omimSuite extends GcdssAlignmentFunSuite {
  test("vcf2omim runSimple, data:small.vcf") {

    //    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    //    val sc = new SparkContext(conf)
    var vcfFile = "file/callDisease/input/vcf/small.vcf"
    var vcfFileAdam = "file/callVariant/output/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype20170409143228184.adam"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimSimple.txt"
    val sd: Option[SequenceDictionary] = None

    //    sc.loadVariants(vcfFileAdam).rdd.take(10).foreach(println)

    val rdd = sc.loadGenotypes(vcfFile)
    println(rdd.rdd.count())
    println(rdd.toVariantContextRDD.rdd.count())
    rdd.rdd.map(_.variant).take(20).foreach(println)

    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    //        println("vcfRDD.head:")
    //        println(vcfRDD.head.genotypes.size)
    println("start call Vcf2Omim")
    val vcf2Omim = new Vcf2Omim(rdd)
    val returnRDD = vcf2Omim.runSimple(sc, vcf2omimSimpleFile)
    println("returnRDD.count:" + returnRDD.count())
    returnRDD.foreach(println)
    /**
      * add by xubo 20160611
      * 主要是由于存储的是逗号，数据里面原先可能有逗号，所以需要区分，改为‘|’为分隔符
      */
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output1 = output + "/simpleT" + iString
    println("output:" + output1)

    val saveRDD = returnRDD.map { each =>
      val str1 = each._1.split(Array(',', '(', ')'))
      val str = str1(1) + '|' + str1(2) + '|' + str1(3) + '|' + str1(4) + '|' + each._2 + '|' + each._3 + '|' + each._4
      str
    }
    //    returnRDD.repartition(1).saveAsTextFile(output)
    saveRDD.repartition(1).saveAsTextFile(output1)

    /** ***********end ***************/
    sc.stop()
  }

  test("vcf2omim runSimple, data:All_20160407L100.vcf") {
    //    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    //    val sc = new SparkContext(conf)
    var vcfFile = "file/callDisease/input/All_20160407L100.vcf"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimSimpleAll.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVcf(vcfFile, sd)
    val rdd = sc.loadGenotypes(vcfFile)
    //    rdd.rdd.map(_.variant).foreach(println)
    //    rdd.map(_.variant.variant).foreach(println)

    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    //        println("vcfRDD.head:")
    //        println(vcfRDD.head.genotypes.size)
    println("start call Vcf2Omim")
    val vcf2Omim = new Vcf2Omim(rdd)
    val returnRDD = vcf2Omim.runSimple(sc, vcf2omimSimpleFile)
    println("returnRDD.count:" + returnRDD.count())
    returnRDD.foreach(println)
    sc.stop()
  }

  test("vcf2omim runSimple,data:All_20160407L10000.vcf") {
    var vcfFile = "file/callDisease/input/All_20160407L10000.vcf"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimSimpleAll.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVcf(vcfFile, sd)
    val rdd = sc.loadGenotypes(vcfFile)
    //    rdd.rdd.map(_.variant).foreach(println)
    //    rdd.map(_.variant.variant).foreach(println)

    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    //        println("vcfRDD.head:")
    //        println(vcfRDD.head.genotypes.size)
    println("start call Vcf2Omim")
    val vcf2Omim = new Vcf2Omim(rdd)
    val returnRDD = vcf2Omim.runSimple(sc, vcf2omimSimpleFile)
    println("returnRDD.count:" + returnRDD.count())
    returnRDD.foreach(println)
    sc.stop()
  }
  test("vcf2omim runSimple, data:All_20160407L1000000.vcf") {
    var vcfFile = "file/callDisease/input/All_20160407L1000000.vcf"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimSimpleAll.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVcf(vcfFile, sd)
    val rdd = sc.loadGenotypes(vcfFile)
    //    rdd.rdd.map(_.variant).foreach(println)
    //    rdd.map(_.variant.variant).foreach(println)

    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    //        println("vcfRDD.head:")
    //        println(vcfRDD.head.genotypes.size)
    println("start call Vcf2Omim")
    val vcf2Omim = new Vcf2Omim(rdd)
    val returnRDD = vcf2Omim.runSimple(sc, vcf2omimSimpleFile)
    println("returnRDD.count:" + returnRDD.count())
    returnRDD.foreach(println)
    sc.stop()
  }
  test("vcf2omim runComplex, data:small.vcf") {
    var vcfFile = "file/callDisease/input/small.vcf"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omim.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVcf(vcfFile, sd)
    //    rdd.map(_.variant.variant).foreach(println)
    val rdd = sc.loadGenotypes(vcfFile)
    rdd.rdd.map(_.variant).foreach(println)
    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    //        println("vcfRDD.head:")
    //        println(vcfRDD.head.genotypes.size)
    println("start call Vcf2Omim")
    val vcf2Omim = new Vcf2Omim(rdd)
    val returnRDD = vcf2Omim.runComplex(sc, vcf2omimSimpleFile)
    println("returnRDD.count:" + returnRDD.count())
    returnRDD.foreach(println)

    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output1 = output + "/ComplexT" + iString
    println("output:" + output1)

    val saveRDD = returnRDD.map { each =>
      val str1 = each._1.split(Array(',', '(', ')'))
      println(str1.length)
      str1.foreach(every => print("|" + every + "| "))
      val str = str1(1) + '|' + str1(2) + '|' + str1(3) + '|' + str1(4) + '|' + each._2 + '|' +
        each._3 + '|' + each._4 + '|' + each._5 + '|' + each._6 + '|' + each._7 + '|' +
        each._8 + '|' + each._9 + '|' + each._10 + '|' + each._11 + '|' + each._12 + '|' +
        each._13 + '|' + each._14 + '|' + each._15
      str
    }
    //    returnRDD.repartition(1).saveAsTextFile(output)
    saveRDD.repartition(1).saveAsTextFile(output1)

    sc.stop()
  }
  test("vcf2omim runComplex, data:All_20160407L100.vcf") {
    var vcfFile = "file/callDisease/input/All_20160407L100.vcf"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimFile = "file/callDisease/input/vcf2omimAll.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVcf(vcfFile, sd)
    val rdd = sc.loadGenotypes(vcfFile)
    //    rdd.rdd.map(_.variant).foreach(println)
    //    rdd.map(_.variant.variant).foreach(println)

    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    //        println("vcfRDD.head:")
    //        println(vcfRDD.head.genotypes.size)
    println("start call Vcf2Omim")
    val vcf2Omim = new Vcf2Omim(rdd)
    val returnRDD = vcf2Omim.runComplex(sc, vcf2omimFile)
    println("returnRDD.count:" + returnRDD.count())
    returnRDD.foreach(println)
    sc.stop()
  }
  test("vcf2omim runComplex,data:All_20160407L10000.vcf") {
    var vcfFile = "file/callDisease/input/All_20160407L10000.vcf"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimFile = "file/callDisease/input/vcf2omimAll.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVcf(vcfFile, sd)
    val rdd = sc.loadGenotypes(vcfFile)
    //    rdd.rdd.map(_.variant).foreach(println)
    //    rdd.map(_.variant.variant).foreach(println)

    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    //        println("vcfRDD.head:")
    //        println(vcfRDD.head.genotypes.size)
    println("start call Vcf2Omim")
    val vcf2Omim = new Vcf2Omim(rdd)
    val returnRDD = vcf2Omim.runComplex(sc, vcf2omimFile)
    println("returnRDD.count:" + returnRDD.count())
    returnRDD.foreach(println)
    sc.stop()
  }

  test("vcf2omim runComplex, data:All_20160407L1000000.vcf") {
    var vcfFile = "file/callDisease/input/All_20160407L1000000.vcf"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimFile = "file/callDisease/input/vcf2omimAll.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVcf(vcfFile, sd)
    val rdd = sc.loadGenotypes(vcfFile)
    //    rdd.rdd.map(_.variant).foreach(println)

    //    rdd.map(_.variant.variant).foreach(println)

    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    //        println("vcfRDD.head:")
    //        println(vcfRDD.head.genotypes.size)
    println("start call Vcf2Omim")
    val vcf2Omim = new Vcf2Omim(rdd)
    val returnRDD = vcf2Omim.runComplex(sc, vcf2omimFile)
    println("returnRDD.count:" + returnRDD.count())
    returnRDD.foreach(println)
    sc.stop()
  }

  test("test data:All_20160407L10000.vcf") {
    var vcfFile = "file/callDisease/input/All_20160407L10000.vcf"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimFile = "file/callDisease/input/vcf2omimAll.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVcf(vcfFile, sd)
    val rdd = sc.loadGenotypes(vcfFile)
    val rddAlternateAllele = rdd.rdd.map { each =>
      each.variant.getAlternateAllele
    }

    rdd.rdd.foreach { each =>
      if ((each.variant.getStart.toInt + 1) == 10493) {
        println(each.variant)
      }
    }
    println(rddAlternateAllele.count())
    rddAlternateAllele.take(10).foreach(println)
    rddAlternateAllele.foreach { each =>
      if (each.split(',').length > 1) {
        println(each)
      }
    }
    sc.stop()

  }

  /**
    * 测试SparkBWA经过变异识别的数据，在使用并行化疾病分析进行处理
    * 状态：运行通过，但是结果返回三个一样的？？？未解决
    */
  test("test data:.sparkBWA/BBg38L50c10000Nhs20Paired12YarnT201606252236LocalNopartition vcf") {
    //    var vcfFile = "file/callDisease/input/small.vcf"
    val path = "hdfs://219.219.220.149:9000/xubo/callVariant/avocado/output/sparkBWA/BBg38L50c10000Nhs20Paired12YarnT201606252236LocalNopartition/SparkBWA_g38L50c10000Nhs20Paired1.fastq-0-NoSort-local-1466932122385-0.samVarNoRG20160627113153282/"

    var output = "file/callDisease/output/vcf2omim/test1"
    //    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimAll.txt"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimAll.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVariantAnnotations(path)
    val rdd = sc.loadGenotypes(path)
    println(rdd.rdd.count())
    println(rdd.rdd.toDebugString)
    rdd.rdd.map(_.getVariant).foreach(println)
    var rdd2 = rdd.rdd.map(_.getVariant).map(each => VariantContext(each))

    /**
      * 手动加上一个进行测试，检测是否运行正常
      * 如何样本中没有omim变异数据，可以用下面的进行手动添加测试
      * 但为什么是3？
      */
    //    var flag = true
    //    rdd2 = rdd2.map { each =>
    //      if (flag == true) {
    //        each.variant.setStart(229432659L)
    //        each.variant.setReferenceAllele("T")
    //        each.variant.setAlternateAllele("C")
    //        flag = false
    //      }
    //      each
    //    }

    println("vatiantContext count:" + rdd2.count())

    //    val rdd0 = sc.loadVcf(vcfFile, sd)

    println("start call Vcf2Omim")
    val vcf2Omim = new Vcf2Omim(rdd)
    val returnRDD = vcf2Omim.runComplex(sc, vcf2omimSimpleFile)
    println("runComplex end")
    println("returnRDD.count:" + returnRDD.count())
    returnRDD.foreach(println)

    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output1 = output + "/ComplexT" + iString
    println("output:" + output1)

    val saveRDD = returnRDD.map { each =>
      val str1 = each._1.split(Array(',', '(', ')'))
      println(str1.length)
      str1.foreach(every => print("|" + every + "| "))
      val str = str1(1) + '|' + str1(2) + '|' + str1(3) + '|' + str1(4) + '|' + each._2 + '|' +
        each._3 + '|' + each._4 + '|' + each._5 + '|' + each._6 + '|' + each._7 + '|' +
        each._8 + '|' + each._9 + '|' + each._10 + '|' + each._11 + '|' + each._12 + '|' +
        each._13 + '|' + each._14 + '|' + each._15
      str
    }
    //    returnRDD.repartition(1).saveAsTextFile(output)
    saveRDD.repartition(1).saveAsTextFile(output1)

    sc.stop()
  }

}
