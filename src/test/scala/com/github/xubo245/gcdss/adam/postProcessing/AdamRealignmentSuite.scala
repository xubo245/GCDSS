package com.github.xubo245.gcdss.adam.postProcessing

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import org.bdgenomics.adam.algorithms.consensus.ConsensusGenerator
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/5.
  */
class AdamRealignmentSuite extends GcdssAlignmentFunSuite {
  test("AdamSort 1") {
    val fqFile = "file/callVariant/input/sam/artificial.sam"
//    val fqFile = "file/callVariant/input/sam/artificial.realigned.sam"
    val alignment = sc.loadAlignments(fqFile)
    alignment.rdd.foreach(println)
    var realignAlignment = alignment.realignIndels(consensusModel = ConsensusGenerator.fromReads)
    println("realignIndels")
    realignAlignment.rdd.foreach(println)

  }
}