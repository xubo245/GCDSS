package com.github.xubo245.gcdss

import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.scalatest.FunSuite

/**
 * Created by xubo on 2016/5/28.
 */
class loadParquetSuite extends FunSuite{
  def resourcePath(path: String) = ClassLoader.getSystemClassLoader.getResource(path).getFile

  def tmpFile(path: String) = Files.createTempDirectory("").toAbsolutePath.toString + "/" + path

 test("callVariant artifical") {
//   sc.stop()
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)
//    val samFile = "output/var20170404212729249"
//    val samLoad = sc.loadAlignments(resourcePath(samFile))
//    val samFile = "C:\\all\\idea\\avocado\\avocado-cli\\src\\test\\resources\\output\\var20170404220409229"
    val samFile = "file/callVariant/output/var20170404230412900"
    val samLoad = sc.loadVariants(samFile)
//    val samFile = "hdfs://219.219.220.149:9000/xubo/callVariant/avocado/output/artificialT20170404192328608"
//    val samLoad = sc.loadAlignments(samFile)
    println("model")
    samLoad.rdd.foreach(println)
    println(samLoad.rdd.count())
    println("end")
    sc.stop
  }

  test("callVariant 2000000") {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)
    val samFile = "file/callVariant/output/var20170404230937574"
    val samLoad = sc.loadVariants(samFile)
    println("model")
    samLoad.rdd.take(10).foreach(println)
    println(samLoad.rdd.count())
    println("end")
    sc.stop
  }
}
