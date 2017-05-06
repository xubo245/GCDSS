spark-submit --class com.github.xubo245.gcdss.utils.FlagStatAdam \
 --master spark://219.219.220.149:7077 \
 --conf "spark.executor.extraJavaOptions=-Djava.library.path=/home/hadoop/disk2/xubo/lib/" \
  --jars /home/hadoop/cloud/adam/lib/adam_2.10-0.21.1-SNAPSHOT.jar \
  --executor-memory 20G \
GCDSS.jar \
/xubo/project/alignment/CloudBWA/g38/time/ERR000589cloudBWAbatch11k2.adam
