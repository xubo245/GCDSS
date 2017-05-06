package com.github.xubo245.gcdss.load

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite

/**
  * Created by xubo on 2017/4/6.
  */
class LoadVCFSuite extends GcdssAlignmentFunSuite {

  test("test:HDFS") {
    sc.stop()
    val vcfFile = "hdfs://219.219.220.149:9000/xubo/callVariant/vcf/vcfSelect.vcf20170406035127942"
    LoadVCF.main(Array(vcfFile))

  }

  test("test") {
    sc.stop()
    val vcfFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionary.adam"
    LoadVCF.main(Array(vcfFile))

  }

  test("test var") {
    sc.stop()
    val vcfFile = "file\\callVariant\\output\\var20170407151810311"
    LoadVCF.main(Array(vcfFile))

  }
  test("test biallelicGenotyper20170407152815299") {
    sc.stop()
    val vcfFile = "file\\callVariant\\output\\biallelicGenotyper20170407152815299"
    LoadVCF.main(Array(vcfFile))

  }
  test("test WithChr") {
    sc.stop()
    val vcfFile = "file\\callVariant\\input\\vcf\\vcfSelectAddSequenceDictionaryWithChr.adam"
    LoadVCF.main(Array(vcfFile))

  }
  test("test variant20170408232535307.vcf") {
    sc.stop()
    val vcfFile = "file\\callVariant\\output\\variant20170409192515495.adam"
    LoadVCF.main(Array(vcfFile))

  }
  test("test load discover variant2") {
    sc.stop()
    val vcfFile = "file\\callVariant\\output\\DiscoverVariant20170409211412241.adam"
    LoadVCF.main(Array(vcfFile))
  }

  test("test:HDFS 2") {
    sc.stop()
    val vcfFile = "hdfs://219.219.220.149:9000/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1DiscoverVariantI1.adam"
    LoadVCF.main(Array(vcfFile))

  }

}
