#for num in 20000000 10000000 2000000
for num in 20000000
do
for core in {1..8}
do

for i in {1..5}
do
#fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.adam'
#fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1DiscoverVariantI'$i'.adam'
out='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time1000num32k1.callDiseaseFromGenotype.I'$i'.adam'
fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time1000num32k1.scalability.DiscoverAndGenotypeI'$i'.adam'
#fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.DiscoverAndGenotypeI'$i'.adam'
vcfDatabase='/xubo/project/CallDisease/input/vcf2omimAll.txt'
hadoop fs -rm -R -f $out
sh testCallDiseaseFromGenotypeScalability.sh $core $fq $vcfDatabase $out
done
done
done
