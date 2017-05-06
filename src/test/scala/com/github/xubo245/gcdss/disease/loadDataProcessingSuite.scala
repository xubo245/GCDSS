package com.github.xubo245.gcdss.disease

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * Created by xubo on 2016/6/9.
 */
class loadDataProcessingSuite extends FunSuite {
  test("test loadDataProcessing.simple") {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = "D:/all/idea/GCDSS/file/callDisease/input/vcf2omimSimple.txt"
    val rdd = loadDataProcessing.simple(sc, file)
    println
    rdd.take(1).foreach(println)
    sc.stop()
  }

  test("test loadDataProcessing.complex,data:vcf2omim") {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = "D:/all/idea/GCDSS/file/callDisease/input/vcf2omim.txt"
    val rdd = loadDataProcessing.complex(sc, file)
    println
    rdd.take(1).foreach(println)
    sc.stop()
  }

  test("test loadDataProcessing.complex,data:vcf2omimAll") {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = "D:/all/idea/GCDSS/file/callDisease/input/vcf2omimAll.txt"
    val rdd = loadDataProcessing.complex(sc, file)
    println
    rdd.foreach(println)

    //    rdd.foreach{each=>
    //      if (each.)
    //
    //    }
    sc.stop()
  }

  test("test data:vcf2omimAll") {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = "D:/all/idea/GCDSS/file/callDisease/input/vcf2omimAll.txt"
    val rdd2 = sc.textFile(file).map(each => each.split('|'))
    rdd2.foreach { each =>
      if (each.length == 17) {} else {
        println(each.length)
        each.foreach { every => print(every + " ")
        }
        println
      }
    }
    sc.stop()
  }

  test("anaysis data,count :vcf2omimAllResult") {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = "D:/all/idea/GCDSS/file/callDisease/input/analysis/vcf2omimAllResult.txt"
    val rdd1 = sc.textFile(file).map(each => each.split('|'))
    //    rdd1.take(10).foreach(println)
    val rdd2 = sc.textFile(file).map(each => each.split(Array('|', '(', ')'))).map { each => each.filter(_ != "") }
    var sum = 0
    rdd2.foreach { each =>
      //      println(each.length)
      //      println(each(0))
      //      println(each(0).split(',').length)
      if (each(0).split(',').length > 4) {
        println(each)
        sum = sum + 1;
      }
      //        each.foreach { every => println(every + " ")
      //      println
    }
    println("rdd2.count:" + rdd2.count())
    println("sum:" + sum)
    sc.stop()
  }

  test("anaysis data,count :vcf2omimAll") {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = "D:/all/idea/GCDSS/file/callDisease/input/vcf2omimAll.txt"
    val rdd1 = sc.textFile(file).map(each => each.split('|'))
    //    rdd1.take(10).foreach(println)
    val rdd2 = sc.textFile(file).map(each => each.split('|'))
    var sum = 0
    val len = rdd2.count()
    rdd2.take(len.toInt).foreach { each =>
      //      println(each.length)
      //      println(each(0))
      //      println(each(0).split(',').length)
      if (each(5).split(',').length > 1) {
        sum = sum + 1;
        println(sum + ":" + each(5))
      }
      //        each.foreach { every => println(every + " ")
      //      println
    }
    println("rdd2.count:" + rdd2.count())
    println("sum:" + sum)
    println((rdd2.count() - sum))
    sc.stop()
  }

  test("anaysis data,read By adam:vcf2omimAll") {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = "D:/all/idea/GCDSS/file/callDisease/input/vcf2omimAll.txt"
    val rdd2 = sc.textFile(file).map(each => each.split('|'))
    rdd2.foreach { each =>
      if (each.length == 17) {} else {
        println(each.length)
        each.foreach { every => print(every + " ")
        }
        println
      }
    }
    sc.stop()
  }

  test("test By changing some value,  function:loadDataProcessing.simple,data:vcf2omimSimple.txt") {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = "D:/all/idea/GCDSS/file/callDisease/input/test/vcf2omimSimple.txt"
    val rdd = loadDataProcessing.simple(sc, file)
    println("rdd println:" + rdd.count())
    rdd.foreach(println)
    sc.stop()
  }
  test("test By changing some value, function:loadDataProcessing.simple ,data:vcf2omim.txt") {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = "D:/all/idea/GCDSS/file/callDisease/input/test/vcf2omim.txt"
    val rdd = loadDataProcessing.complex(sc, file)
    println("rdd println:" + rdd.count())
    rdd.foreach(println)
    sc.stop()
  }

  test("anaysis data,count :vcf2omimSimpleAllResult") {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = "D:/all/idea/GCDSS/file/callDisease/input/vcf2omimSimpleAllResult.txt"
    val rdd = sc.textFile(file).map(each => each.split('|'))
    //    rdd1.take(10).foreach(println)
    //    val rdd = sc.textFile(file).map(each => each.split(Array('|')))

    println("all:" + rdd.count())
    rdd.take(10).foreach { each =>
      each.foreach(every => print(every + " "))
      println
    }

    //    rdd.map(_ (5)).foreach(println)
    println("omim distinct count:" + rdd.map(_(5)).distinct().count())
    println("dbSnpId distinct count:" + rdd.map(_(6)).distinct().count())

    //DataProcessing
    println("vcf2omimSimpleAll:")
    val file2 = "D:/all/idea/GCDSS/file/callDisease/input/vcf2omimSimpleAll.txt"
    val rdd2 = sc.textFile(file2).map(each => each.split('|'))
    println("rdd2.count()" + rdd2.count())
    println("omim distinct count:" + rdd2.map(_(0)).distinct().count())
    println("dbSnpId distinct count:" + rdd2.map(_(1)).distinct().count())
    sc.stop()
  }
}
