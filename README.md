# OMOP2OWL-vocab
Convert OMOP vocab into OWL and SemanticSQL.

## Prerequisites
* [Python 3.9+](https://www.python.org/downloads/)
* [Java 11+](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html)
* Docker

## Installation
1. `pip install omop2owl-vocab`
2. `omop2owl-vocab --install`

## Dev installation
1. Set up a virtual environment and activate it.
2. Run: `make install`

## Running
Run: `omop2owl-vocab OPTIONS`

### CLI
```
omop2owl-vocab --help
usage: omop2owl-vocab [-h] [-c CONCEPT_CSV_PATH] [-r CONCEPT_RELATIONSHIP_CSV_PATH] [-O OUTDIR] [-I ONTOLOGY_ID] [-o {merged,split,merged-post-split,rxnorm}]
                      [-v VOCABS [VOCABS ...]] [-R RELATIONSHIPS [RELATIONSHIPS ...]] [-S] [-e] [-s] [-C] [-M MEMORY] [-i]

Convert OMOP vocabularies to OWL and SemanticSQL.

options:
  -h, --help            show this help message and exit
  -c CONCEPT_CSV_PATH, --concept-csv-path CONCEPT_CSV_PATH
                        Path to CSV of OMOP concept table.
  -r CONCEPT_RELATIONSHIP_CSV_PATH, --concept-relationship-csv-path CONCEPT_RELATIONSHIP_CSV_PATH
                        Path to CSV of OMOP concept_relationship table.
  -O OUTDIR, --outdir OUTDIR
                        Output directory.
  -I ONTOLOGY_ID, --ontology-id ONTOLOGY_ID
                        Identifier for ontology. Used to generate a pURL and file name.
  -o {merged,split,merged-post-split,rxnorm}, --output-type {merged,split,merged-post-split,rxnorm}
                        What output to generate? If "merged" will create an ONTOLOGY_ID.db file with all concepts of all vocabs merged into one. If "split" will
                        create an ONTOLOGY_ID-*.db file for each vocab. "merged-post-split" output will be as if running both "split" and "merged", but the
                        merging implementation is different. Use this option if running out of memory. If using "rxnorm", will create a specifically customized
                        ONTOLOGY_ID-RxNorm.db.
  -v VOCABS [VOCABS ...], --vocabs VOCABS [VOCABS ...]
                        Used with `--output-type specific-vocabs-merged`. Which vocabularies to include in the output? Usage: --vocabs "Procedure Type" "Device
                        Type"
  -R RELATIONSHIPS [RELATIONSHIPS ...], --relationships RELATIONSHIPS [RELATIONSHIPS ...]
                        Which relationship types from the concept_relationship table's relationship_id field to include? Default is "Is a" only. Passing "ALL"
                        includes everything. Ignored for --output-type options that are specific to a pre-set vocabulary (e.g. rxnorm). Usage: --realationships
                        "Is a" "Maps to"
  -S, --skip-semsql     In addition to .owl, also convert to a SemanticSQL .db? This is always True except when --output-type is all-merged-post-split and it is
                        creating initial .owl files to be merged.
  -e, --exclude-singletons
                        Exclude terms that do not have any relationships. This only applies to --method robot.
  -s, --semsql-only     Use this if the .owl already exists and you just want to create a SemanticSQL .db.
  -C, --use-cache       Of outputs or intermediates already exist, use them.
  -M MEMORY, --memory MEMORY
                        The amount of Java memory (GB) to allocate.
  -i, --install         Installs necessary docker images.
```
