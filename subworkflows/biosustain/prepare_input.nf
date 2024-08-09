/*
 * Check input samplesheet and get read channels
 */

include { COMPILE_SAMPLESHEET } from '../../modules/biosustain/compile_samplesheet'

workflow PREPARE_INPUT {
    take:
    benchling_samplesheet
    input_path

    main:
    /*
     * Check samplesheet is valid
     */
    COMPILE_SAMPLESHEET ( benchling_samplesheet, input_path )
        .csv
        .set { samplesheet }

    emit:
    samplesheet // [ group, replicate, barcode, input_file, genome, transcriptome ]
}

// Function to resolve fasta and gtf file if using iGenomes
// Returns [ sample, input_file, barcode, fasta, gtf, is_transcripts, annotation_str, nanopolish_fast5 ]
def get_sample_info(LinkedHashMap sample, LinkedHashMap genomeMap) {
    def meta = [:]
    meta.id  = sample.sample

    // Resolve fasta and gtf file if using iGenomes
    def fasta = false
    def gtf   = false
    if (sample.fasta) {
        if (genomeMap && genomeMap.containsKey(sample.fasta)) {
            fasta = file(genomeMap[sample.fasta].fasta, checkIfExists: true)
            gtf   = file(genomeMap[sample.fasta].gtf, checkIfExists: true)
        } else {
            fasta = file(sample.fasta, checkIfExists: true)
        }
    }

    // Check if input file and gtf file exists
    input_file = sample.input_file ? file(sample.input_file, checkIfExists: true) : null
    gtf        = sample.gtf        ? file(sample.gtf, checkIfExists: true)        : gtf

    return [ meta, input_file, sample.barcode, fasta, gtf, sample.is_transcripts.toBoolean(), fasta.toString()+';'+gtf.toString(), sample.nanopolish_fast5 ]
}
