package com.github.xubo245.gcdss.adam.postProcessing

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.cli.Transform
import org.bdgenomics.adam.models.SnpTable
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variant.VariantRDD

/**
  * Created by xubo on 2017/4/6.
  */
class AdamBQSRSuite extends GcdssAlignmentFunSuite {

  test("test") {
    sc.stop()
    val conf = new SparkConf().setAppName("KnownVCF")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.serializer", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
          .set("spark.kryo.serializer", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
          .set("spark.kryo.referenceTracking", "true")
          .set("spark.kryo.registrationRequired", "true")
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      conf.setMaster("local[16]")
    }
    val sparkContext = new SparkContext(conf)
    val ac = new ADAMContext(sparkContext)
    println("BQSRRDD:")
    //    val fqFile = "file/callVariant/input/sam/artificial.realigned.sam"
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"

    //    val fqFile = "file/callVariant/input/sam/artificial.sam"
    val alignment = sparkContext.loadAlignments(fqFile)
//    alignment.rdd.take(10).foreach(println)
    //    val vcfFile = "file/callVariant/input/vcf/vcfSelect.adam"
    val vcfFile = "file/callVariant/input/vcf/vcfSelectAddSequenceDictionary.adam"
    val variants = sparkContext.loadVariants(vcfFile)
    //    val variants: RDD[RichVariant] = sc.loadVariants(vcfFile).rdd.map(new RichVariant(_))
    //    println("variants")
    //    variants.rdd.map(_.getContigName).collect().foreach{each=>print(each+" ")}
    var variantsChr = variants.rdd.map { each =>
      var name = each.getContigName
      //      if (!(name.equalsIgnoreCase("X") || name.equalsIgnoreCase("Y"))) {
      each.setContigName("chr" + name)
      //      }
      each
    }
   var varRDD= VariantRDD(variantsChr, variants.sequences, variants.headerLines)
    val snps = sparkContext.broadcast(SnpTable(varRDD))
    //    val snps = sc.broadcast(SnpTable(VariantRDD(variantsChr, alignment.sequences, variants.headerLines)))
//    val BQSRRDD = alignment.recalibateBaseQualities(snps)
    //    BQSRRDD.rdd.take(10).foreach(println)0
//    alignment.recalibrateBaseQualities()
//    println(BQSRRDD.rdd.count())
    sparkContext.stop()
  }

  test("test 2"){
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    val out = "file/callVariant/output/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.transform.adam"
    Transform(Array("-single", "-sort_reads", "-sort_lexicographically", fqFile, out)).run(sc)
  }

  test("test sort"){
    val fqFile = "file/callVariant/input/sam/unordered.sam"
    val out = "file/callVariant/output/sam/ordered.sam"
    Transform(Array("-single", "-sort_reads", "-sort_lexicographically", fqFile, out)).run(sc)
  }

  test("test recalibrate_base_qualities"){
    val fqFile = "file/callVariant/input/sam/unordered.sam"
    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualities.sam"
    Transform(Array("-single", "-sort_reads", "-sort_lexicographically","-recalibrate_base_qualities", fqFile, out)).run(sc)
  }

  test("test recalibrate_base_qualities with known_snps"){
    val fqFile = "file/callVariant/input/sam/unordered.chr.sam"
    val vcfFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionaryWithChr.adam"
    val out = "file/callVariant/output/sam/orderedrecalibrate_base_qualities.sam"
    Transform(Array("-single", "-sort_reads", "-sort_lexicographically","-recalibrate_base_qualities","-known_snps",vcfFile, fqFile, out)).run(sc)
  }
  test("test realign_indels"){
    val fqFile = "file/callVariant/input/sam/unordered.sam"
    val out = "file/callVariant/output/sam/realign_indels.sam"
    Transform(Array("-single", "-sort_reads", "-sort_lexicographically","-recalibrate_base_qualities","-realign_indels", fqFile, out)).run(sc)
  }
  test("test mark_duplicate_reads"){
    val fqFile = "file/callVariant/input/sam/unordered.sam"
    val out = "file/callVariant/output/sam/mark_duplicate_reads.sam"
    Transform(Array("-single", "-sort_reads", "-sort_lexicographically","-recalibrate_base_qualities","-realign_indels","-mark_duplicate_reads", fqFile, out)).run(sc)
  }
  test("test cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"){
    val vcfFile = "file/callVariant/input/vcf/vcfSelectAddSequenceDictionary.adam"
    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    val out = "file/callVariant/output/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.transform.adam"
    Transform(Array("-single", "-sort_reads","-known_snps",vcfFile, "-sort_lexicographically","-recalibrate_base_qualities","-realign_indels","-mark_duplicate_reads", fqFile, out)).run(sc)
  }
}
