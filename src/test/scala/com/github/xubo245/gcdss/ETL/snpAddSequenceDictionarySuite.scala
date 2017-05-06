package com.github.xubo245.gcdss.ETL

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite

/**
  * Created by xubo on 2017/4/6.
  */
class snpAddSequenceDictionarySuite extends GcdssAlignmentFunSuite{
  test("test"){
    sc.stop()
    val samFIle="file/callVariant/input/sam/bwamemSRR062634k2L500.sam"
    var varFile="file/callVariant/input/vcf/vcfSelect.adam"
    var out="file/callVariant/input/vcf/vcfSelectAddSequenceDictionaryWithChr.adam"
    snpAddSequenceDictionary.main(Array(samFIle,varFile,out))
  }

}
