package com.github.xubo245.gcdss.disease

import java.text.SimpleDateFormat
import java.util.Date

import com.github.xubo245.gcdss.utils.{Constants, GcdssAlignmentFunSuite}
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.ADAMContext._
/**
  * Created by xubo on 2017/4/9.
  */
class CallDiseaseSuite extends GcdssAlignmentFunSuite{
  test("test"){

    var vcfFile = "file/callDisease/input/vcf/small.vcf"
    var vcfFileAdam = "file/callVariant/output/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype20170409143228184.adam"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimSimple.txt"
    val sd: Option[SequenceDictionary] = None

    val rdd = sc.loadGenotypes(vcfFile)
    println(rdd.rdd.count())
    println(rdd.toVariantContextRDD.rdd.count())
    rdd.rdd.map(_.variant).take(20).foreach(println)

    println("start call Vcf2Omim")
    val callDisease = new CallDisease(rdd.toVariantContextRDD)
    val returnRDD = callDisease.runSimple(sc, vcf2omimSimpleFile)
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
    saveRDD.repartition(1).saveAsTextFile(output1)
    sc.stop()
  }

  test("test 2"){
    Constants.debug=false
    var vcfFile = "file/callDisease/input/vcf/small.vcf"
    var vcfFileAdam = "file/callVariant/output/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype20170409143228184.adam"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimSimple.txt"
    val sd: Option[SequenceDictionary] = None

    val rdd = sc.loadGenotypes(vcfFile)

    val callDisease = new CallDisease(rdd.toVariantContextRDD)
    val returnRDD = callDisease.runSimple(sc, vcf2omimSimpleFile)

    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output1 = output + "/simpleT" + iString

    val saveRDD = returnRDD.map { each =>
      val str1 = each._1.split(Array(',', '(', ')'))
      val str = str1(1) + '|' + str1(2) + '|' + str1(3) + '|' + str1(4) + '|' + each._2 + '|' + each._3 + '|' + each._4
      str
    }
    saveRDD.foreach(println)
    saveRDD.repartition(1).saveAsTextFile(output1)
    sc.stop()
  }

  test("test 3 from variant"){
    //success
    Constants.debug=false
//    var vcfFile = "file/callDisease/input/vcf/small.vcf"
//    var vcfFileAdam = "file/callVariant/output/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype20170409143228184.adam"
    val vcfFile = "file\\callVariant\\output\\DiscoverVariant20170409211412241.adam"
    var output = "file/callDisease/output/vcf2omim/test1"
//    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimSimple.txt"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimSimpleAll.txt"
    val sd: Option[SequenceDictionary] = None

    val rdd = sc.loadVariants(vcfFile)

    val callDisease = new CallDisease(rdd.toVariantContextRDD)
    val returnRDD = callDisease.runSimple(sc, vcf2omimSimpleFile)

    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output1 = output + "/simpleT" + iString

    val saveRDD = returnRDD.map { each =>
      val str1 = each._1.split(Array(',', '(', ')'))
      val str = str1(1) + '|' + str1(2) + '|' + str1(3) + '|' + str1(4) + '|' + each._2 + '|' + each._3 + '|' + each._4
      str
    }
    saveRDD.foreach(println)
    saveRDD.repartition(1).saveAsTextFile(output1)
    sc.stop()
  }

  test("test adam"){
    Constants.debug=false
//    var vcfFile = "file/callDisease/input/vcf/small.vcf"
    var vcfFileAdam = "file/callVariant/output/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype20170409143228184.adam"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimSimpleAll.txt"
    val sd: Option[SequenceDictionary] = None

    val rdd = sc.loadGenotypes(vcfFileAdam)

    val callDisease = new CallDisease(rdd.toVariantContextRDD)
    val returnRDD = callDisease.runSimple(sc, vcf2omimSimpleFile)

    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output1 = output + "/simpleT" + iString

    val saveRDD = returnRDD.map { each =>
      val str1 = each._1.split(Array(',', '(', ')'))
      val str = str1(1) + '|' + str1(2) + '|' + str1(3) + '|' + str1(4) + '|' + each._2 + '|' + each._3 + '|' + each._4
      str
    }
    println("*************count:"+saveRDD.count())
    saveRDD.foreach(println)
    saveRDD.repartition(1).saveAsTextFile(output1)
    sc.stop()
  }

  test("test adam:From HDFS"){
    Constants.debug=false
    //    var vcfFile = "file/callDisease/input/vcf/small.vcf"
//    var vcfFileAdam = "file/callVariant/output/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype20170409143228184.adam"
    val vcfFileAdam = "hdfs://219.219.220.149:9000/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c20000000Nhs20Paired12time10num16k1.DiscoverAndGenotypeI1.adam"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimSimpleAll.txt"
    val sd: Option[SequenceDictionary] = None

    val rdd = sc.loadGenotypes(vcfFileAdam)

    rdd.rdd.take(100).foreach(println)
    val fliterRDD=rdd.rdd.filter{each=>
    each.variant.getAlternateAllele.length==1
    }
    println("*****************"+fliterRDD.count())
      fliterRDD.take(100).foreach(println)
    val callDisease = new CallDisease(rdd.toVariantContextRDD)
    val returnRDD = callDisease.runSimple(sc, vcf2omimSimpleFile)

    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output1 = output + "/simpleT" + iString

    val saveRDD = returnRDD.map { each =>
      val str1 = each._1.split(Array(',', '(', ')'))
      val str = str1(1) + '|' + str1(2) + '|' + str1(3) + '|' + str1(4) + '|' + each._2 + '|' + each._3 + '|' + each._4
      str
    }
    println("*************count:"+saveRDD.count())
    saveRDD.foreach(println)
    saveRDD.repartition(1).saveAsTextFile(output1)
    sc.stop()
  }

  test("vcf2omim runComplex, data:small.vcf") {
    Constants.debug=false
    var vcfFile = "file/callDisease/input/vcf/small.vcf"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omim.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVcf(vcfFile, sd)
    //    rdd.map(_.variant.variant).foreach(println)
    val rdd = sc.loadGenotypes(vcfFile)
    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    //        println("vcfRDD.head:")
    //        println(vcfRDD.head.genotypes.size)
    println("start call Vcf2Omim")
    val vcf2Omim = new CallDisease(rdd.toVariantContextRDD)
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
      val str = str1(1) + '|' + str1(2) + '|' + str1(3) + '|' + str1(4) + '|' + each._2 + '|' + each._3 + '|'  + "http://omim.org/entry/"+each._3 + '|' + each._4 + '|' + each._5 + '|' + each._6 + '|' + each._7 + '|' +
        each._8 + '|' + each._9 + '|' + each._10 + '|' + each._11 + '|' + each._12 + '|' +
        each._13 + '|' + each._14 + '|' + each._15
      str
    }
    //    returnRDD.repartition(1).saveAsTextFile(output)
    saveRDD.repartition(1).saveAsTextFile(output1)

    sc.stop()
  }

  test("vcf2omim runComplex, data:variant") {
    Constants.debug=false
//    var vcfFile = "file/callDisease/input/vcf/small.vcf"
    val vcfFile = "file\\callVariant\\output\\DiscoverVariant20170409211412241.adam"
    var output = "file/callDisease/output/vcf2omim/test1"
    val vcf2omimSimpleFile = "file/callDisease/input/vcf2omimAll.txt"
    val sd: Option[SequenceDictionary] = None
    //    val rdd = sc.loadVcf(vcfFile, sd)
    //    rdd.map(_.variant.variant).foreach(println)
    val rdd = sc.loadVariants(vcfFile)
    //        val vcfRDD = sc.loadGenotypes(vcfFile).toVariantContext.collect.sortBy(_.position)
    //        println("vcfRDD.head:")
    //        println(vcfRDD.head.genotypes.size)
    println("start call Vcf2Omim")
    val vcf2Omim = new CallDisease(rdd.toVariantContextRDD)
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
    println("count:"+saveRDD.count())
    saveRDD.foreach(println)
    saveRDD.repartition(1).saveAsTextFile(output1)

    sc.stop()
  }
}
