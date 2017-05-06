package com.github.xubo245.gcdss.adam.postProcessing

import java.text.SimpleDateFormat
import java.util.Date

import com.github.xubo245.gcdss.utils.ADAMFunSuite
import org.bdgenomics.adam.cli.Transform
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/6.
  */
class AdamTest extends ADAMFunSuite{
//  sparkTest("test"){
//    println("hello")
//  }
//  sparkTest("test recalibrate_base_qualities with known_snps 2"){
//    val fqFile = "file/callVariant/input/sam/unordered.chr.sam"
//    val vcfFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionaryWithChr.adam"
//    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualities.sam"
//    Transform(Array("-single", "-sort_reads", "-sort_lexicographically","-recalibrate_base_qualities","-known_snps",vcfFile, fqFile, out)).run(sc)
//  }

  sparkTest("test adam:cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.rg.adam"){
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.rg.adam"
    val vcfFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionaryWithChr.adam"
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualities"+iString+".sam"
    Transform(Array("-single", "-sort_reads", "-sort_lexicographically","-recalibrate_base_qualities","-known_snps",vcfFile, fqFile, out)).run(sc)
  }

//  sparkTest("test adam:cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.rg.adam"){
//    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.rg.adam"
//    val vcfFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionaryWithChr.adam"
//    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualitiescloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.rg.sam"
//    Transform(Array("-single", "-sort_reads", "-sort_lexicographically","-recalibrate_base_qualities","-known_snps",vcfFile, fqFile, out)).run(sc)
//  }
}
