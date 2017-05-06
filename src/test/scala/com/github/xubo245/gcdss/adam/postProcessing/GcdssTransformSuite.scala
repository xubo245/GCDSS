package com.github.xubo245.gcdss.adam.postProcessing

import java.text.SimpleDateFormat
import java.util.Date

import com.github.xubo245.gcdss.utils.ADAMFunSuite

/**
  * Created by xubo on 2017/4/6.
  */
class GcdssTransformSuite extends ADAMFunSuite {

  sparkTest("test:cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.rg.adam") {
    sc.stop()
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.rg.adam"
    val vcfFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionaryWithChr.adam"
    //    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualities.sam"
    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualitiescloudBWAnewg38L50c10Nhs20Paired12time10num16k1.rg" + iString + ".sam"
    GcdssTransform.main(Array(fqFile, out, vcfFile))
  }
  sparkTest("test:cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.rg.adam") {
    sc.stop()
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.rg.adam"
    val vcfFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionaryWithChr.adam"
    //    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualities.sam"
    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualitiescloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.rg" + iString + ".sam"
    GcdssTransform.main(Array(fqFile, out, vcfFile))
  }
}
