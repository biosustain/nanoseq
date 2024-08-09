process COMPILE_SAMPLESHEET {
    tag "$samplesheet"
    label 'process_single'

    conda "conda-forge::python=3.8.3"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/python:3.8.3' :
        'quay.io/biocontainers/python:3.8.3' }"

    input:
    path benchling_samplesheet
    val input_path

    output:
    path '*.csv'       , emit: csv
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script: // This script is bundled with the pipeline, in nf-core/nanoseq/bin/
    updated_path = workflow.profile.contains('test_nodx_rnamod') ? "$input_path" : "not_changed"
    """
    pip install pyarrow
    biosustain_compile_samplesheet.py \\
        "$benchling_samplesheet" \\
        "$updated_path" \\
        '${params.input_file_template}' \\
        "samplesheet.csv"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version | sed 's/Python //g')
    END_VERSIONS
    """
}
