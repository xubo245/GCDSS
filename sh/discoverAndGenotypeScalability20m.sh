#for num in 2000000 10000000 20000000
for num in 20000000
do
for core in {1..8}
do

for i in {1..5}
do
fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time1000num32k1.adam'
out='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time1000num32k1.scalability.DiscoverAndGenotypeI'$i'.adam'
vcf='/xubo/callVariant/vcf/vcfSelectAddSequenceDictionaryWithChr.adam'

hadoop fs -rm -R -f $out
sh testDiscoverAndGenotypeScalability.sh $core $fq $out
done
done
done
