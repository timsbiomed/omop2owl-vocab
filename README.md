# N3C OWL Ingest
Ingestion of N3C's OMOP data source into OWL and SemanticSQL.

## Prerequisites
* Python 3.9+
* Docker
* [`robot`](http://robot.obolibrary.org/) (if using instead of YARRRML)

## Installation
Run: `make install`

## Running
### Run with defaults
Run: `make all`

### CLI

```
python -m n3c_owl_ingest --help
usage: Creates TSVs and of unmapped terms as well as summary statistics. [-h]
                                                                         [-o {all-merged,all-split,all-merged-post-split,rxnorm,specific-vocabs-merged}]
                                                                         [-c CONCEPT_CSV_PATH]
                                                                         [-r CONCEPT_RELATIONSHIP_CSV_PATH]
                                                                         [-m {robot,yarrrml}]
                                                                         [-v VOCABS [VOCABS ...]] [-S]
                                                                         [-s] [-C]

options:
  -h, --help            show this help message and exit
  -o {all-merged,all-split,all-merged-post-split,rxnorm,specific-vocabs-merged}, --output-type {all-merged,all-split,all-merged-post-split,rxnorm,specific-vocabs-merged}
                        What output to generate? If "all-merged" will create an n3c.db file with all
                        concepts of all vocabs merged into one. If "all-split" will create an n3c-*.db
                        file for each vocab. If "rxnorm" will create a specifically customized
                        n3c-RxNorm.db.
  -c CONCEPT_CSV_PATH, --concept-csv-path CONCEPT_CSV_PATH
                        Path to CSV of OMOP concept table.
  -r CONCEPT_RELATIONSHIP_CSV_PATH, --concept-relationship-csv-path CONCEPT_RELATIONSHIP_CSV_PATH
                        Path to CSV of OMOP concept_relationship table.
  -m {robot,yarrrml}, --method {robot,yarrrml}
                        What tooling / method to use to generate output?
  -v VOCABS [VOCABS ...], --vocabs VOCABS [VOCABS ...]
                        Used with `--output-type specific-vocabs-merged`. Which vocabularies to include
                        in the output?
  -S, --skip-semsql     In addition to .owl, also convert to a SemanticSQL .db? This is always True
                        except when --output-type is all-merged-post-split and it is creating initial
                        .owl files to be merged.
  -s, --semsql-only     Use this if the .owl already exists and you just want to create a SemanticSQL
                        .db.
  -C, --use-cache       Of outputs or intermediates already exist, use them

```