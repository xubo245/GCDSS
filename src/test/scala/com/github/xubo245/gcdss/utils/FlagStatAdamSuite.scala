package com.github.xubo245.gcdss.utils

/**
  * Created by xubo on 2017/4/7.
  */
class FlagStatAdamSuite extends GcdssAlignmentFunSuite {

  test("test") {
    sc.stop()
    val file = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch11k1.adam"
    FlagStatAdam.main(Array(file))
  }

  test("testERR000589L10000cloudBWAbatch110k1") {
    sc.stop()
    //    val file = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch11k1.adam"
    val file = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch110k1.adam"
    FlagStatAdam.main(Array(file))

    //    20000 + 0 in total (QC-passed reads + QC-failed reads)
    //    0 + 0 primary duplicates
    //    0 + 0 primary duplicates - both read and mate mapped
    //    0 + 0 primary duplicates - only read mapped
    //    0 + 0 primary duplicates - cross chromosome
    //      0 + 0 secondary duplicates
    //    0 + 0 secondary duplicates - both read and mate mapped
    //    0 + 0 secondary duplicates - only read mapped
    //    0 + 0 secondary duplicates - cross chromosome
    //      19883 + 0 mapped (99.42%:0.00%)
    //    20000 + 0 paired in sequencing
    //      10000 + 0 read1
    //      10000 + 0 read2
    //      19722 + 0 properly paired (98.61%:0.00%)
    //    19828 + 0 with itself and mate mapped
    //      55 + 0 singletons (0.28%:0.00%)
    //    66 + 0 with mate mapped to a different chr
    //      22 + 0 with mate mapped to a different chr (mapQ>=5)
    //    - testERR000589L10000cloudBWAbatch110k1
  }
  test("testERR000589L10000cloudBWAbatch120k1") {
    sc.stop()
    //    val file = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch11k1.adam"
    val file = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch120k1.adam"
    FlagStatAdam.main(Array(file))
  }

  test("testERR000589L10000cloudBWAbatch130k1") {
    sc.stop()
    val file = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch130k1.adam"
    FlagStatAdam.main(Array(file))
  }
  test("cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam") {
    sc.stop()
    val file = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    FlagStatAdam.main(Array(file))
  }
}
