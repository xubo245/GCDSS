spark-submit --class com.github.xubo245.gcdss.disease.CallDiseaseFromGenotype \
 --master spark://219.219.220.149:7077 \
 --conf "spark.executor.extraJavaOptions=-Djava.library.path=/home/hadoop/disk2/xubo/lib/" \
  --jars /home/hadoop/cloud/adam/lib/adam_2.10-0.23.0-SNAPSHOT.jar,/home/hadoop/cloud/adam/lib/avocado-core-0.0.3-SNAPSHOT.jar,/home/hadoop/cloud/adam/lib/avocado-cli-0.0.3-SNAPSHOT.jar \
  --executor-memory 20G \
GCDSS.jar \
$1 $2 $3

 #--jars /home/hadoop/cloud/adam/lib/adam_2.10-0.21.1-SNAPSHOT.jar,/home/hadoop/cloud/adam/lib/avocado-core-0.0.3-SNAPSHOT.jar,/home/hadoop/cloud/adam/lib/avocado-cli-0.0.3-SNAPSHOT.jar \
#/xubo/callVariant/vcf/All_20160407.vcf \
#/xubo/callVariant/vcf/All_20160407L10000.vcf \
