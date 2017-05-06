package com.github.xubo245.gcdss

import java.text.SimpleDateFormat
import java.util._

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.avocado.cli.AvocadoMain
import org.bdgenomics.avocado.genotyping.BiallelicGenotyper
import org.bdgenomics.avocado.cli.{BiallelicGenotyper => BiallelicGenotypercli}
import org.bdgenomics.formats.avro.GenotypeAllele
import org.scalatest.FunSuite

/**
  * Created by xubo on 2016/5/15.
  */

class AvocadoMainSuite extends GcdssAlignmentFunSuite {
  def resourcePath(path: String) = ClassLoader.getSystemClassLoader.getResource(path).getFile

  test("file/callVariant/input/sam/NA12878_snp_A2G_chr20_225058.sam") {
    //    val conf = new SparkConf().setAppName("AvocadoSuite").setMaster("local[16]")
    //    val sc = new SparkContext(conf)
    //    val ac = new ADAMContext(sc)
    val fqFile = "file/callVariant/input/sam/NA12878_snp_A2G_chr20_225058.sam"

    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output = "file/callVariant/output/variant" + iString+".adam"

    sc.stop()
    AvocadoMain.main(Array("discover", fqFile, output))
    //    sc.stop()
  }

  test("discover:file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam") {
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output = "file/callVariant/output/DiscoverVariant" + iString + ".adam"

    sc.stop()
    AvocadoMain.main(Array("discover", fqFile, output))
    //    sc.stop()
  }

  test("biallelicGenotyper") {
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    val vcfFile = "file\\callVariant\\output\\variant20170408232535307.vcf"
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output = "file/callVariant/output/biallelicGenotyper" + iString + ".adam"
    sc.stop()
    //    sc.stop()
    AvocadoMain.main(Array("biallelicGenotyper", fqFile, output))
    //    AvocadoMain.main(Array("biallelicGenotyper", vcfFile, output))
    //    sc.stop()
  }

  test("BiallelicGenotypercli") {
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output = "file/callVariant/output/biallelicGenotyper" + iString + ".adam"
    //    sc.stop()
    //    sc.stop()
    BiallelicGenotypercli(Array("-min_observations_to_discover_variant","-1","-min_genotype_quality","-1",fqFile, output)).run(sc)
    //    AvocadoMain.main(Array("biallelicGenotyper", vcfFile, output))
    //    sc.stop()
  }

  test("biallelicGenotyper:variant") {
    //    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    val fqFile = "file\\callVariant\\output\\variant20170408233357696.adam"
    val vcfFile = "file\\callVariant\\output\\variant20170408232535307.vcf"
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output = "file/callVariant/output/biallelicGenotyper" + iString + ".adam"
    sc.stop()
    //    sc.stop()
    AvocadoMain.main(Array("biallelicGenotyper", "-variants_to_call", "", fqFile, output))
    //    AvocadoMain.main(Array("biallelicGenotyper", vcfFile, output))
    //    sc.stop()
  }





  test("load Genotype") {
    var ge = sc.loadGenotypes("file/callVariant/output/biallelicGenotyper20170409133450814.adam")
    println(ge.rdd.count())
    ge.rdd.take(10).foreach(println)
  }

  test("load cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype20170409143228184.adam") {
    var ge = sc.loadGenotypes("file/callVariant/output/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype20170409143228184.adam")
    println(ge.rdd.count())
    ge.rdd.take(10).foreach(println)
  }
  test("load HDFS") {
//    var ge = sc.loadGenotypes("hdfs://219.219.220.149:9000/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.DiscoverAndGenotypeI1.adam")
//    var ge = sc.loadGenotypes("hdfs://219.219.220.149:9000/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c20000000Nhs20Paired12time10num16k1.DiscoverAndGenotypeI1.adam")
    var ge = sc.loadGenotypes("hdfs://219.219.220.149:9000/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c10000000Nhs20Paired12time10num16k1.DiscoverAndGenotypeI1.adam")
    println(ge.rdd.count())
    ge.rdd.take(10).foreach(println)
  }
  test("file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam") {
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output = "file/callVariant/output\\var" + iString
    //        sc.stop()
    AvocadoMain.main(Array("discover", fqFile, output))
    //        sc.stop()
  }
}
