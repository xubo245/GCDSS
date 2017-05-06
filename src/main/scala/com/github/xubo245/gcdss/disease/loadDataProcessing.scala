/**
 * @author xubo
 *         more code:https://github.com/xubo245/SparkLearning
 *         more blog:http://blog.csdn.net/xubo245
 */
package com.github.xubo245.gcdss.disease

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xubo on 2016/5/23.
 */
object loadDataProcessing {
  def simple(sc: SparkContext, file: String): RDD[((String, String, String, String), String, String)] = {
    val rdd = sc.textFile(file)
    //    val rdd2 = sc.textFile(file).map(each => each.split(Array(',', '(', ')')).filter { every => every != "" })
    val rdd2 = sc.textFile(file).map(each => each.split('|'))
    val rdd31 = rdd2.map { each =>
      val alternateAllele = each(5).split(',')
      if (alternateAllele.length > 0) {
        ((each(2), each(3), each(4), alternateAllele(0)), each(0), each(1))
      } else {
        ((each(2), each(3), each(4), each(5)), each(0), each(1))
        //        <((each(2), each(3), each(4), each(5)), each(0), each(1)),((each(2), each(3), each(4), each(5)), each(0), each(1))>
      }
    }

    val rdd32 = rdd2.filter(each => each(5).split(',').length > 1).map { each =>
      val alternateAllele = each(5).split(',')
      ((each(2), each(3), each(4), alternateAllele(1)), each(0), each(1))
    }
    val rdd33 = rdd2.filter(each => each(5).split(',').length > 2).map { each =>
      val alternateAllele = each(5).split(',')
      ((each(2), each(3), each(4), alternateAllele(2)), each(0), each(1))
    }
    val rdd34 = rdd2.filter(each => each(5).split(',').length > 3).map { each =>
      val alternateAllele = each(5).split(',')
      ((each(2), each(3), each(4), alternateAllele(3)), each(0), each(1))
    }
    val rdd3 = rdd31.union(rdd32).union(rdd33).union(rdd34)
    //    rdd2.foreach { each =>
    //      each.foreach(every => print(every + " "))
    //    }
    rdd3
  }

  def complex(sc: SparkContext, file: String): RDD[((String, String, String, String), (String, String, String, String, String, String, String, String, String, String, String, String, String))] = {
    val rdd = sc.textFile(file)
    val rdd2 = sc.textFile(file).map(each => each.split('|'))
    rdd2.take(1).foreach(println)
    //    rdd2.foreach(each => println(each.length))
    val rdd3 = rdd2.map { each =>
      if (each.length == 17) {
        ((each(2), each(3), each(4), each(5)), (each(0), each(1), each(6), each(7), each(8), each(9), each(10), each(11), each(12), each(13), each(14), each(15), each(16)))
      } else {
        //有两条数据长度为16
        //        16
        //        107580 rs121909574 6 10404509 T C,G omim 6.60 6 27 08 6p24.3 TFAP2A, AP2TF, BOFS C Transcription factor AP-2 alpha (activating enhancer-binding protein 2 alpha)
        //        16
        //        107580 rs121909575 6 10402590 C T omim 6.60 6 27 08 6p24.3 TFAP2A, AP2TF, BOFS C Transcription factor AP-2 alpha (activating enhancer-binding protein 2 alpha)
        ((each(2), each(3), each(4), each(5)), (each(0), each(1), each(6), each(7), each(8), each(9), each(10), each(11), each(12), each(13), each(14), each(15), "no data"))
      }
    }

    val rdd41 = rdd3.filter(_._1._4.split(',').length > 0).map { each =>
      val alternateAllele = each._1._4.split(',')
      ((each._1._1, each._1._2, each._1._3, alternateAllele(0)), each._2)
    }
    val rdd42 = rdd3.filter(_._1._4.split(',').length > 1).map { each =>
      val alternateAllele = each._1._4.split(',')
      ((each._1._1, each._1._2, each._1._3, alternateAllele(1)), each._2)
    }
    val rdd43 = rdd3.filter(_._1._4.split(',').length > 2).map { each =>
      val alternateAllele = each._1._4.split(',')
      ((each._1._1, each._1._2, each._1._3, alternateAllele(2)), each._2)
    }
    val rdd44 = rdd3.filter(_._1._4.split(',').length > 3).map { each =>
      val alternateAllele = each._1._4.split(',')
      ((each._1._1, each._1._2, each._1._3, alternateAllele(3)), each._2)
    }

    //    rdd2.foreach { each =>
    //      each.foreach(every => print(every + " "))
    //    }
    val rdd5 = rdd41.union(rdd42).union(rdd43).union(rdd44)
    rdd5
    //    rdd3
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    //    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    println("start:")
    val startTime = System.currentTimeMillis()

    val file = "file/callDisease/input/vcf2omimSimple.txt"
    val rdd = sc.textFile(file)
    //      .map(each => each.split("\\s+"))

    println("rdd.count():" + rdd.count())
    rdd.take(10).foreach(println)

    val rdd2 = sc.textFile(file).map(each => each.split(Array(',', '(', ')')).filter {
      every => every != ""
    })
    rdd2.foreach {
      each =>
        each.foreach(every => print(every + " "))
        println
        println(each.length)
        println("***A" + each(0) + "B*******")
        println(each(1))
        println(each(2))
    }

    println("\n file2:")
    val file2 = "file/callDisease/input/vcf2omim.txt"
    val rdd3 = sc.textFile(file2)
    rdd3.foreach(println)
    val rdd4 = sc.textFile(file2).map(each => each.split(Array(',', '(', ')')).filter {
      every => every != ""
    })
    rdd4.foreach {
      each =>
        each.foreach(every => print(every + " "))
        println
        println(each.length)
        println("***A" + each(0) + "B*******")
        println(each(1))
        println(each(2))
    }
    val loadTime = System.currentTimeMillis()
    println("load time:" + (loadTime - startTime) + " ms")

    val saveTime = System.currentTimeMillis()
    println("save time:" + (saveTime - loadTime) + " ms")
    println("run time:" + (saveTime - startTime) + " ms")

    sc.stop
  }
}
