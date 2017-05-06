#for num in 2000000 10000000 20000000
for num in 2000000 4000000 6000000 8000000 10000000 12000000 14000000 16000000 18000000 20000000
do
for i in {1..5}
do
fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time1000num32k1.adam'
out='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time1000num32k1DiscoverVariantI'$i'.adam'

hadoop fs -rm -R -f $out
sh testDiscoverVariant.sh $fq $out
done
done
