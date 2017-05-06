package com.github.xubo245.gcdss.adam.postProcessing

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._
/**
  * Created by xubo on 2017/4/5.
  */
class AdamMarkDuplicatesSuite extends GcdssAlignmentFunSuite {

  test("test artificial") {

    val fqFile = "file/callVariant/input/sam/artificial.sam"
    //    val fqFile = "file/callVariant/input/sam/artificial.realigned.sam"
    val alignment = sc.loadAlignments(fqFile)
    alignment.rdd.foreach(println)
    var MD = alignment.markDuplicates()
    println("markDuplicates")
    MD.rdd.foreach(println)
  }

  test("test cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam") {

    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    //    val fqFile = "file/callVariant/input/sam/artificial.realigned.sam"
    val alignment = sc.loadAlignments(fqFile)
    alignment.rdd.take(10).foreach(println)
    println(alignment.rdd.count())
    var MD = alignment.markDuplicates()
    println("markDuplicates")
    MD.rdd.take(10).foreach(println)
    println("true")
    val dupMD=MD.rdd.filter(_.getDuplicateRead==true)
    dupMD.take(10).foreach(println)
    println("dupMD:"+dupMD.count())
    println(MD.rdd.count())
//    dupMD:44
//    4000000
  }
}
