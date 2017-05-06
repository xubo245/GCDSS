package com.github.xubo245.gcdss.avocado

import java.text.SimpleDateFormat
import java.util.Date

import com.github.xubo245.gcdss.utils.GcdssAlignmentFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._

/**
  * Created by xubo on 2017/4/9.
  */
class DiscoverAndGenotypeSuite extends GcdssAlignmentFunSuite {
  test("as") {
    sc.stop()
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    var fq = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    var out = "file/callVariant/output/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype" + iString + ".adam"
    DiscoverAndGenotype.main(Array(fq, out))
//
//    var genotype = sc.loadGenotypes(out)
//    var oneRDD = genotype.rdd.filter { each =>
//      each.getVariant.getAlternateAllele.length == 1
//    }
//
//    println("******:"+oneRDD.count())
  }

  test("DiscoverAndGenotype and load") {
    sc.stop()
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    var fq = "file/callVariant/input/sam/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam"
    var out = "file/callVariant/output/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype" + iString + ".adam"
    DiscoverAndGenotype.main(Array(fq, out))


  }

  test("load"){
    var out="file/callVariant/output/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotype20170409213241634.adam"
    var genotype = sc.loadGenotypes(out)
    var oneRDD = genotype.rdd.filter { each =>
      each.getVariant.getAlternateAllele.length == 1
    }
    var twoRDD = genotype.rdd.filter { each =>
      each.getVariant.getAlternateAllele.length == 2
    }
    var threeRDD = genotype.rdd.filter { each =>
      each.getVariant.getAlternateAllele.length == 3
    }

    println("******genotype:"+genotype.rdd.count())
    genotype.rdd.take(20).foreach(println)
    println("******oneRDD:"+oneRDD.count())
    println("******twoRDD:"+twoRDD.count())
    twoRDD.take(20).foreach(println)
    println("******threeRDD:"+threeRDD.count())
    threeRDD.take(20).foreach(println)
  }
  test("load:HDFS"){
    var out="hdfs://219.219.220.149:9000/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c20000000Nhs20Paired12time10num16k1.DiscoverAndGenotypeTestI1.adam"
    var genotype = sc.loadGenotypes(out)
    var oneRDD = genotype.rdd.filter { each =>
      each.getVariant.getAlternateAllele.length == 1
    }
    var twoRDD = genotype.rdd.filter { each =>
      each.getVariant.getAlternateAllele.length == 2
    }
    var threeRDD = genotype.rdd.filter { each =>
      each.getVariant.getAlternateAllele.length == 3
    }

    println("******genotype:"+genotype.rdd.count())
    genotype.rdd.take(20).foreach(println)
    println("******oneRDD:"+oneRDD.count())
    println("******twoRDD:"+twoRDD.count())
    twoRDD.take(20).foreach(println)
    println("******threeRDD:"+threeRDD.count())
    threeRDD.take(20).foreach(println)
  }
  test("load:HDFS 2"){
    var out="hdfs://219.219.220.149:9000/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.DiscoverAndGenotypeI1.adam"
    var genotype = sc.loadGenotypes(out)
    var oneRDD = genotype.rdd.filter { each =>
      each.getVariant.getAlternateAllele.length == 1
    }
    var twoRDD = genotype.rdd.filter { each =>
      each.getVariant.getAlternateAllele.length == 2
    }
    var threeRDD = genotype.rdd.filter { each =>
      each.getVariant.getAlternateAllele.length == 3
    }

    println("******genotype:"+genotype.rdd.count())
    genotype.rdd.take(20).foreach(println)
    println("******oneRDD:"+oneRDD.count())
    println("******twoRDD:"+twoRDD.count())
    twoRDD.take(20).foreach(println)
    println("******threeRDD:"+threeRDD.count())
    threeRDD.take(20).foreach(println)
  }
}
