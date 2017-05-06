for num in 6000000 8000000 12000000 14000000 16000000 18000000 10000000 20000000
#for num in {12000000..18000000..2000000}
do
for core in {1..8}
do

for i in {1..5}
do
fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time1000num32k1.adam'
out='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time1000num32k1.scalability.DiscoverVariantI'$i'.adam'

hadoop fs -rm -R -f $out
sh testDiscoverVariantScalability.sh $core $fq $out
done
done
done
