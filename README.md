# N3C OWL Ingest
Ingestion of N3C's OMOP data source into OWL and SemanticSQL.

## Prerequisites
* Python 3.9+
* Docker
* [`robot`](http://robot.obolibrary.org/) (if not using `--method yarrrml`)

## Installation
Run: `make install`

## Running
### Run with defaults
Run: `make all`

### CLI
```
python -m n3c_owl_ingest --help
usage: Creates TSVs and of unmapped terms as well as summary statistics. [-h]
                                                                         [-o {merged,split,merged-post-split,rxnorm}]
                                                                         [-c CONCEPT_CSV_PATH]
                                                                         [-r CONCEPT_RELATIONSHIP_CSV_PATH]
                                                                         [-m {robot,yarrrml}]
                                                                         [-v VOCABS [VOCABS ...]]
                                                                         [-R RELATIONSHIPS [RELATIONSHIPS ...]]
                                                                         [-S] [-k] [-s] [-C]

options:
  -h, --help            show this help message and exit
  -o {merged,split,merged-post-split,rxnorm}, --output-type {merged,split,merged-post-split,rxnorm}
                        What output to generate? If "merged" will create an n3c.db file with all concepts of all
                        vocabs merged into one. If "split" will create an n3c-*.db file for each vocab. "merged-
                        post-split" output will be as if running both "split" and "merged", but the merging
                        implementation is different. Use this option if running out of memory. If using
                        "rxnorm", will create a specifically customized n3c-RxNorm.db.
  -c CONCEPT_CSV_PATH, --concept-csv-path CONCEPT_CSV_PATH
                        Path to CSV of OMOP concept table.
  -r CONCEPT_RELATIONSHIP_CSV_PATH, --concept-relationship-csv-path CONCEPT_RELATIONSHIP_CSV_PATH
                        Path to CSV of OMOP concept_relationship table.
  -m {robot,yarrrml}, --method {robot,yarrrml}
                        What tooling / method to use to generate output?
  -v VOCABS [VOCABS ...], --vocabs VOCABS [VOCABS ...]
                        Used with `--output-type specific-vocabs-merged`. Which vocabularies to include in the
                        output? Usage: --vocabs "Procedure Type" "Device Type"
  -R RELATIONSHIPS [RELATIONSHIPS ...], --relationships RELATIONSHIPS [RELATIONSHIPS ...]
                        Which relationship types from the concept_relationship table's relationship_id field to
                        include? Default is "Subsumes" only. Passing "ALL" includes everything. Ignored for
                        --output-type options that are specific to a pre-set vocabulary (e.g. rxnorm). Usage:
                        --realationships "Subsumes" "Maps to"
  -S, --skip-semsql     In addition to .owl, also convert to a SemanticSQL .db? This is always True except when
                        --output-type is all-merged-post-split and it is creating initial .owl files to be
                        merged.
  -k, --keep-singletons
                        Currently, to help with the very high memory demands of processing all of N3C OMOP as a
                        single OWL, and because immediate needs of users do not require singletons (classes with
                        no relationships), they are being left out by default. Adding this flag will keep them.
                        This only applies to --method robot. Since there is no memory issue with --method
                        yarrrml, they will always be retained there.
  -s, --semsql-only     Use this if the .owl already exists and you just want to create a SemanticSQL .db.
  -C, --use-cache       Of outputs or intermediates already exist, use them.
```
