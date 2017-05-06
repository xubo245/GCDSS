package com.github.xubo245.gcdss.ETL

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite

/**
  * Created by xubo on 2017/4/5.
  */
class KnownVCFSuite extends GcdssAlignmentFunSuite {
  test("KnownVCF") {
    sc.stop()
    val vcfFile = "file\\callVariant\\input\\vcf\\All_20160407L10000.vcf"
    //    val vcfFile = "hdfs://219.219.220.149:9000/xubo/callVariant/vcf/All_20160407.vcf"
    val vcf2omimFile = "file\\callVariant\\input\\vcf\\vcf2omimAllResult.txt"
    val out = "file\\callVariant\\input\\vcf\\vcfSelect.vcf"
    KnownVCF.main(Array(vcfFile, vcf2omimFile,out))
  }

}
