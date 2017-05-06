package com.github.xubo245.gcdss.load

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import htsjdk.samtools.util.Log
import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/6.
  */
class LoadSequenceDictionarySuite extends GcdssAlignmentFunSuite {
  test("test fastq") {
    sc.stop()
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    LoadSequenceDictionary.main(Array(fqFile))
  }

  test("test fastq：bwamemSRR062634k2L500.sam") {
    sc.stop()
    val fqFile = "file/callVariant/input/sam/bwamemSRR062634k2L500.sam"
    LoadSequenceDictionary.main(Array(fqFile))
  }

  test("test adam：vcfSelectAddSequenceDictionary.adam") {
    //    val fqFile = "file/callVariant/input/sam/bwamemSRR062634k2L500.sam"
    val fqFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionary.adam"
    LoadSequenceDictionary.computeVCF(sc, fqFile)
  }
  test("test adam：vcfSelectAddSequenceDictionaryWithChr.adam") {
    //    val fqFile = "file/callVariant/input/sam/bwamemSRR062634k2L500.sam"
    val fqFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionaryWithChr.adam"
    LoadSequenceDictionary.computeVCF(sc, fqFile)
  }


  test("test fastq:GCA_000001405.15_GRCh38_full_analysis_set.fasta") {
    sc.stop()
    val fqFile = "file\\ref\\GRCH38Index/GCA_000001405.15_GRCh38_full_analysis_set.fasta"
    LoadSequenceDictionary.main(Array(fqFile))
  }

  test("test fastq:file/callVariant/input/sam/ERR000589L10000cloudBWAbatch110k1.adam") {
    sc.stop()
    val fqFile = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch110k1.adam"
    LoadSequenceDictionary.main(Array(fqFile))
  }
}
