for i in {6..9..1}
do
fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.adam'
out='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c2000000Nhs20Paired12time10num16k1.genotypeI'$i'.adam'
vcf='/xubo/callVariant/vcf/vcfSelectAddSequenceDictionaryWithChr.adam'
hadoop fs -rm -R -f $out
sh testDiscoverAndGenotype.sh $fq $out
done
