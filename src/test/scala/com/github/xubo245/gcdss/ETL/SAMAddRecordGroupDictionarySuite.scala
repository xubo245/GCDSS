package com.github.xubo245.gcdss.ETL

import com.github.xubo245.gcdss.utils.ADAMFunSuite

/**
  * Created by xubo on 2017/4/6.
  */
class SAMAddRecordGroupDictionarySuite extends ADAMFunSuite{

//  sparkTest("test"){
//    sc.stop()
//    var fq="file/callVariant/input/sam/cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.adam"
//    var out="file/callVariant/input/sam/cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.rg.adam"
//    SAMAddRecordGroupDictionary.main(Array(fq,out))
//  }

  sparkTest("test:Big"){
    sc.stop()
    var fq="file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    var out="file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.rg.adam"
    SAMAddRecordGroupDictionary.main(Array(fq,out))
  }

}
