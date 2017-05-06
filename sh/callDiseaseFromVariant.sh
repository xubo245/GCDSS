for num in 2000000 10000000 20000000
do
for i in {1..1}
do
#fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.adam'
fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1DiscoverVariantI'$i'.adam'
out='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.callDiseaseFromVariantI'$i'.adam'
vcfDatabase='/xubo/project/CallDisease/input/vcf2omimAll.txt'
hadoop fs -rm -R -f $out
sh testCallDiseaseFromVariant.sh $fq $vcfDatabase $out
done
done
