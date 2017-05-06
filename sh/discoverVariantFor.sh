for num in 2000000 10000000 20000000
do
for i in {2..6}
do
fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.adam'
out='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1DiscoverVariantI'$i'.adam'

hadoop fs -rm -R -f $out
sh testDiscoverVariant.sh $fq $out
done
done
