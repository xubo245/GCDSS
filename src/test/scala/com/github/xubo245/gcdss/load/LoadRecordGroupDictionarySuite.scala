package com.github.xubo245.gcdss.load

import com.github.xubo245.gcdss.utils.ADAMFunSuite

/**
  * Created by xubo on 2017/4/6.
  */
class LoadRecordGroupDictionarySuite extends ADAMFunSuite {
//  sparkTest("test") {
//    sc.stop()
//    val fqFile = "file/callVariant/input/sam/artificial.realigned.sam"
//    LoadRecordGroupDictionary.main(Array(fqFile))
//  }
//
//  sparkTest("test sam:NA12878_snp_A2G_chr20_225058.sam") {
//    sc.stop()
//    val fqFile = "file/callVariant/input/sam/NA12878_snp_A2G_chr20_225058.sam"
//    LoadRecordGroupDictionary.main(Array(fqFile))
//  }
//
//  sparkTest("test adam:cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.adam") {
//    sc.stop()
//    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.adam"
//    LoadRecordGroupDictionary.main(Array(fqFile))
//  }
//  sparkTest("test adam:cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.rg.adam") {
//    sc.stop()
//    val fqFile = "file/callVariant/input/sam/cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.rg.adam"
//    LoadRecordGroupDictionary.main(Array(fqFile))
//  }

  sparkTest("test adam:cloudBWAnewg38L50c10Nhs20Paired12time10num16k1.adam after add RG by CloudBWA") {
    sc.stop()
    val fqFile = "file/callVariant/input/sam/addRG/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    LoadRecordGroupDictionary.main(Array(fqFile))
  }
}
