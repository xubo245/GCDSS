package com.github.xubo245.gcdss.avocado

import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.avocado.cli.{BiallelicGenotyper => BiallelicGenotypercli, DiscoverVariants, AvocadoMain}

/**
  * Created by xubo on 2017/4/9.
  */
object DiscoverVariant {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    var sam = args(0)
    var out = args(1)
    var appArgs = "sam:" + sam + "\tout:" + out
    val conf = new SparkConf().setAppName("DiscoverVariant:" + appArgs)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
      //      .set("spark.kryo.registrator", "org.bdgenomics.avocado.serialization.AvocadoKryoRegistrator")
      .set("spark.kryo.referenceTracking", "true")
    //      .set("spark.kryoserializer.buffer", "4M")
    //      .registerKryoClasses(Array(classOf[Option[_]], classOf[Integer]))
    //      .registerKryoClasses(Array(classOf[Option[Integer]], classOf[Integer], classOf[Option[_]], classOf[Option[Int]]))
    //    scala.reflect.ManifestFactory
    //      .registerKryoClasses(Array(classOf[Option[Int]],classOf[Option[Integer]],classOf[scala.Option],classOf[java.lang.Integer]))
    //      .set("spark.kryo.registrationRequired", "true")
    //      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      conf.setMaster("local[16]")
    }
    val sc = new SparkContext(conf)
    val ac = new ADAMContext(sc)


    //        AvocadoMain.main(Array("discover", sam, out))
    DiscoverVariants(Array(sam, out)).run(sc)

    //    compute(sc, fqFile, out, vcfFile)
    sc.stop
    val stopTime = System.currentTimeMillis()

    println(appArgs + "\ttime:\t" + (stopTime - startTime) / 1000.0 + "\t")

  }


}
