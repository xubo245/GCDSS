# GCDSS
GCDSS: Distributed Gene Clinical Decision Support System Based on Cloud Computing

## 1 Building GCDSS

####1.1 Preparing: we should prepare before run GCDSS.

- Apache Spark  : https://github.com/apache/spark
- Alluxio: http://www.alluxio.org/
- HDFS: https://github.com/apache/hadoop 

##2.build
	mvn -DskipTests clean package
##3.run

	spark-submit --class com.github.xubo245.gcdss.adam.postProcessing.ReadPostProcessing \
	 --master spark://MaspterIP:7077 \
	 --conf "spark.executor.extraJavaOptions=-Djava.library.path=/home/hadoop/disk2/xubo/lib/" \
	  --jars /home/hadoop/cloud/adam/lib/adam_2.10-0.23.0-SNAPSHOT.jar \
	  --executor-memory 20G \
	GCDSS.jar \
	$1 $2 $3 $4

Parameter:

	$1: operate type, such as markDuplicate,sort,BQSR，realignIndel
    $2: reads file location
    $3: output path

For Example:

	for num in 2000000 10000000 20000000
	do
	for i in {1..5}
	do
	fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.adam'
	out='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.markDuplicateI'$i'.adam'
	vcf='/xubo/callVariant/vcf/vcfSelectAddSequenceDictionaryWithChr.adam'
	
	hadoop fs -rm -R -f $out
	sh testReadPostProcessing.sh "markDuplicate" $fq $out $vcf
	hadoop fs -rm -R -f $out
	done
	done

more shell example in [sh](./sh) file 

## 4.Append
   CloudBWA is a distributed read mapping algorithms in GCDSS

   The code of CloudBWA in another github project: [CloudBWA](https://github.com/xubo245/CloudBWA) 

## Tutorial

the Tutorial or docs is being written.

## Help

If you have any questions or suggestions, please write it in the issue of this project or send an e-mail to me: xubo245@mail.ustc.edu.cn