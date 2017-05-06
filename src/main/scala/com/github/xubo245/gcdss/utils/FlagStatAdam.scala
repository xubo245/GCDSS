package com.github.xubo245.gcdss.utils

import htsjdk.samtools.util.Log
import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.FlagStatMetrics

/**
  * Created by xubo on 2017/4/7.
  */
object FlagStatAdam {
  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("FlagStatAdam")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .set("spark.kryo.referenceTracking", "true")
      .set("spark.kryo.registrationRequired", "true")
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      conf.setMaster("local[16]")
    }
    Log.setGlobalLogLevel(Log.LogLevel.ERROR)
    //    LoadRecordGroupDictionary
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
        val file=args(0)
//    val file = "file/callVariant/input/sam/ERR000589L10000cloudBWAbatch11k1.adam"
//    val file = "alluxio://Master:19998/xubo/project/alignment/CloudBWA/g38/time/ERR000589cloudBWAbatch11k1.adam"
    compute(sc, file)
  }

  def compute(sc: SparkContext, file: String): Unit = {
    val RDD=sc.loadAlignments(file)
    val (failedVendorQuality, passedVendorQuality) =RDD.flagStat()
    def percent(fraction: Long, total: Long) = if (total == 0) 0.0 else 100.00 * fraction.toFloat / total
    val output = """
                   |%d + %d in total (QC-passed reads + QC-failed reads)
                   |%d + %d primary duplicates
                   |%d + %d primary duplicates - both read and mate mapped
                   |%d + %d primary duplicates - only read mapped
                   |%d + %d primary duplicates - cross chromosome
                   |%d + %d secondary duplicates
                   |%d + %d secondary duplicates - both read and mate mapped
                   |%d + %d secondary duplicates - only read mapped
                   |%d + %d secondary duplicates - cross chromosome
                   |%d + %d mapped (%.2f%%:%.2f%%)
                   |%d + %d paired in sequencing
                   |%d + %d read1
                   |%d + %d read2
                   |%d + %d properly paired (%.2f%%:%.2f%%)
                   |%d + %d with itself and mate mapped
                   |%d + %d singletons (%.2f%%:%.2f%%)
                   |%d + %d with mate mapped to a different chr
                   |%d + %d with mate mapped to a different chr (mapQ>=5)
                 """.stripMargin('|').trim.format(
      passedVendorQuality.total, failedVendorQuality.total,
      passedVendorQuality.duplicatesPrimary.total, failedVendorQuality.duplicatesPrimary.total,
      passedVendorQuality.duplicatesPrimary.bothMapped, failedVendorQuality.duplicatesPrimary.bothMapped,
      passedVendorQuality.duplicatesPrimary.onlyReadMapped, failedVendorQuality.duplicatesPrimary.onlyReadMapped,
      passedVendorQuality.duplicatesPrimary.crossChromosome, failedVendorQuality.duplicatesPrimary.crossChromosome,
      passedVendorQuality.duplicatesSecondary.total, failedVendorQuality.duplicatesSecondary.total,
      passedVendorQuality.duplicatesSecondary.bothMapped, failedVendorQuality.duplicatesSecondary.bothMapped,
      passedVendorQuality.duplicatesSecondary.onlyReadMapped, failedVendorQuality.duplicatesSecondary.onlyReadMapped,
      passedVendorQuality.duplicatesSecondary.crossChromosome, failedVendorQuality.duplicatesSecondary.crossChromosome,
      passedVendorQuality.mapped, failedVendorQuality.mapped,
      percent(passedVendorQuality.mapped, passedVendorQuality.total),
      percent(failedVendorQuality.mapped, failedVendorQuality.total),
      passedVendorQuality.pairedInSequencing, failedVendorQuality.pairedInSequencing,
      passedVendorQuality.read1, failedVendorQuality.read1,
      passedVendorQuality.read2, failedVendorQuality.read2,
      passedVendorQuality.properlyPaired, failedVendorQuality.properlyPaired,
      percent(passedVendorQuality.properlyPaired, passedVendorQuality.total),
      percent(failedVendorQuality.properlyPaired, failedVendorQuality.total),
      passedVendorQuality.withSelfAndMateMapped, failedVendorQuality.withSelfAndMateMapped,
      passedVendorQuality.singleton, failedVendorQuality.singleton,
      percent(passedVendorQuality.singleton, passedVendorQuality.total),
      percent(failedVendorQuality.singleton, failedVendorQuality.total),
      passedVendorQuality.withMateMappedToDiffChromosome, failedVendorQuality.withMateMappedToDiffChromosome,
      passedVendorQuality.withMateMappedToDiffChromosomeMapQ5, failedVendorQuality.withMateMappedToDiffChromosomeMapQ5
    )
    println(output)


  }
}
