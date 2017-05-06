package com.github.xubo245.gcdss.avocado

import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.cli.Transform
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMContext

/**
  * Created by xubo on 2017/4/9.
  */
object SAM2Adam {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    var sam = args(0)
    var out = args(1)
    var appArgs = "sam:" + sam + "\tout:" + out
    val conf = new SparkConf().setAppName("SAM2Adam:" + appArgs)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      .set("spark.kryo.referenceTracking", "true")
      .set("spark.kryo.registrationRequired", "true")
    //      .setMaster("local[16]")
    //    if (System.getProperties.getProperty("os.name").contains("Windows")) {
    //      conf.setMaster("local[16]")
    //    }
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)
    compute(sc, sam, out)
    sc.stop
    val stopTime = System.currentTimeMillis()
    println(appArgs + "\ttime:\t" + (stopTime - startTime) / 1000.0 + "\t")
  }

  def compute(sc: SparkContext, sam: String, out: String): Unit = {
    sc.loadAlignments(sam).saveAsParquet(out)
  }
}
