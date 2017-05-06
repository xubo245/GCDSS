package com.github.xubo245.gcdss.adam.postProcessing

import java.text.SimpleDateFormat
import java.util.Date

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import org.bdgenomics.adam.cli.Transform

/**
  * Created by xubo on 2017/4/8.
  */
class ReadPostProcessingSuite extends GcdssAlignmentFunSuite {
  val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
  test("test sort") {
    val fqFile = "file/callVariant/input/sam/unordered.sam"
//    val out = "file/callVariant/output/sam/ordered.sam"+iString
    val out = "file/callVariant/output/sam/ordered"+iString+".sam"
    ReadPostProcessing.sort(sc, fqFile, out)
  }
  test("test recalibrate_base_qualities with known_snps") {
    val fqFile = "file/callVariant/input/sam/unordered.chr.sam"
    val vcfFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionaryWithChr.adam"
//    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualities.sam"
    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualities.sam"
    ReadPostProcessing.BQSR(sc, fqFile, out,vcfFile)
  }
  test("test realign_indels") {
    val fqFile = "file/callVariant/input/sam/unordered.sam"
//    val out = "file/callVariant/output/sam/realign_indels.sam"
    val out = "file/callVariant/output/sam/realign_indels"+iString+".sam"
    ReadPostProcessing.realignIndel(sc, fqFile, out)
  }
  test("test mark_duplicate_reads") {
    val fqFile = "file/callVariant/input/sam/unordered.sam"
//    val out = "file/callVariant/output/sam/mark_duplicate_reads.sam"
    val out = "file/callVariant/output/sam/mark_duplicate_reads"+iString+".sam"
    ReadPostProcessing.sort(sc, fqFile, out)
  }

}
