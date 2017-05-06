package org.bdgenomics.avocado.genotyping

import java.text.SimpleDateFormat
import java.util.Date

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import org.bdgenomics.avocado.cli.AvocadoMain
import org.bdgenomics.formats.avro.GenotypeAllele
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/9.
  */
class BiallelicGenotyperSuite extends GcdssAlignmentFunSuite {
  test("biallelicGenotyper:2") {
    val readPath = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    //    val readPath = "file\\callVariant\\output\\variant20170408233357696.adam"
    val vcfFile = "file\\callVariant\\output\\variant20170408232535307.vcf"
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output = "file/callVariant/output/biallelicGenotyper" + iString + ".adam"
    //    sc.stop()
    //    sc.stop()
    //    AvocadoMain.main(Array("biallelicGenotyper", "-variants_to_call", "", fqFile, output))
    //    AvocadoMain.main(Array("biallelicGenotyper", vcfFile, output))
    //    sc.stop()
    val reads = sc.loadAlignments(readPath)
    //      .transform(rdd => {
    //        rdd.filter(_.getMapq > 10)
    //      })

    val gts = BiallelicGenotyper.discoverAndCall(reads,
      2,
      optPhredThreshold = Some(25))
      //      .transform(rdd => {
      ////      rdd.filter(gt => !gt.getAlleles.forall(_ == GenotypeAllele.REF))
      //      rdd
      //    })
      .rdd.collect

    gts.take(10).foreach(println)
    println(gts.length)
  }
  test("biallelicGenotyper:3") {
    val readPath = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    val vcfFile = "file\\callVariant\\output\\variant20170408232535307.vcf"
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val output = "file/callVariant/output/biallelicGenotyper" + iString + ".adam"
    val reads = sc.loadAlignments(readPath)

    val gts = BiallelicGenotyper.discoverAndCall(reads, 2)
      .rdd.collect

    gts.take(10).foreach(println)
    println(gts.length)
  }
}
