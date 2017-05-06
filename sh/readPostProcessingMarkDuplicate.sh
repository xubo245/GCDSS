for num in 2000000 10000000 20000000
do
for i in {1..5}
do
fq='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.adam'
out='/xubo/project/alignment/CloudBWA/g38/time/cloudBWAnewg38L50c'$num'Nhs20Paired12time10num16k1.markDuplicateI'$i'.adam'
vcf='/xubo/callVariant/vcf/vcfSelectAddSequenceDictionaryWithChr.adam'

hadoop fs -rm -R -f $out
sh testReadPostProcessing.sh "markDuplicate" $fq $out $vcf
hadoop fs -rm -R -f $out
done
done
