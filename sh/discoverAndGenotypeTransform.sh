for num in 2000000 10000000 20000000
do
for i in {1..10}
do
fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.transformI'$i'.adam'
out='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.transform.DiscoverVariantI'$i'.adam'

hadoop fs -rm -R -f $out
sh testDiscoverAndGenotype.sh $fq $out
done
done
