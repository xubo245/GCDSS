package com.github.xubo245.gcdss.load

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/6.
  */
class LoadAlignmentRecordSuite extends GcdssAlignmentFunSuite {
  test("test") {

    sc.stop()
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    LoadAlignmentRecord.main(Array(fqFile))
  }

  test("test analysis") {

    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    val fqRDD = sc.loadAlignments(fqFile)
    fqRDD.rdd.take(10).foreach(println)
    println(fqRDD.rdd.count())
  }

  //  C:\Users\xubo\Desktop\GCDSS\data\ERR000589.adam

  test("test analysis: file/callVariant/input/sam/ERR000589.adam") {

    val fqFile = "file/callVariant/input/sam/ERR000589.adam"
    val fqRDD = sc.loadAlignments(fqFile)
    fqRDD.rdd.take(10).foreach(println)
    println(fqRDD.rdd.count())
  }

  test("test analysis: file/callVariant/input/sam/ERR000589cloudBWAbatch11k1.adam") {

    val fqFile = "file/callVariant/input/sam/ERR000589cloudBWAbatch11k1.adam"
    val fqRDD = sc.loadAlignments(fqFile)
    fqRDD.rdd.take(10).foreach(println)
    println()
    println("*******count:" + fqRDD.rdd.count() + "********")
    println()
  }

  test("test analysis: file/callVariant/input/sam/ERR000589.L10000.adam") {

    val fqFile = "file/callVariant/input/sam/ERR000589.L10000.adam"
    val fqRDD = sc.loadAlignments(fqFile)
    fqRDD.rdd.take(10).foreach(println)
    println()
    println("*******count:" + fqRDD.rdd.count() + "********")
    println()
  }

  test("test analysis: file/callVariant/input/sam/ERR000589L10000cloudBWAbatch11k1.adam") {

    val fqFile = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch11k1.adam"
    val fqRDD = sc.loadAlignments(fqFile)
    fqRDD.rdd.take(10).foreach(println)
    println()
    println("*******count:" + fqRDD.rdd.count() + "********")
    println()
  }
  test("test analysis: file/callVariant/input/sam/ERR000589L10000cloudBWAbatch110k1.adam") {

    val fqFile = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch110k1.adam"
    val fqRDD = sc.loadAlignments(fqFile)
    fqRDD.rdd.take(10).foreach(println)
    println()
    println("*******count:" + fqRDD.rdd.count() + "********")
    println()
  }

  test("test analysis: file/callVariant/input/sam/ERR000589L10000cloudBWAbatch120k1.adam") {

    val fqFile = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch120k1.adam"
    val fqRDD = sc.loadAlignments(fqFile)
    fqRDD.rdd.take(10).foreach(println)
    println()
    println("*******count:" + fqRDD.rdd.count() + "********")
    println()
  }
}
