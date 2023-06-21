# N3C OWL Ingest
Ingestion of N3C's OMOP data source into OWL and SemanticSQL.

## Prerequisites
* Python 3.9+
* Docker
* [`robot`](http://robot.obolibrary.org/) (if not using `--method yarrrml`)
* [Git Large File Storage (LFS)](https://git-lfs.com/)
* A large amount of memory (see in "caveats")

## Installation
1. Set up a virtual environment and activate it.
2. Run: `make install`  
This will install everything for `--method robot`. If using `--method robot`, do `make install-yarrrml-method`.

## Running
### Run with defaults
Run: `make all`

### Caveats
#### Memory requirements
Running with defaults takes somewhere between 28-50gb, and this only includes the "Subsumes" relationship type. There 
are 411 total relationship types, thusly requiring more memory as you add more. 

#### `--method yarrrml`
This is in development and currently generates incorrect output.

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
                                                                         [-S] [-e] [-s] [-C]

options:
  -h, --help            show this help message and exit
  -o {merged,split,merged-post-split,rxnorm}, --output-type {merged,split,merged-post-split,rxnorm}
                        What output to generate? If "merged" will create an n3c.db file with all
                        concepts of all vocabs merged into one. If "split" will create an n3c-*.db file
                        for each vocab. "merged-post-split" output will be as if running both "split"
                        and "merged", but the merging implementation is different. Use this option if
                        running out of memory. If using "rxnorm", will create a specifically customized
                        n3c-RxNorm.db.
  -c CONCEPT_CSV_PATH, --concept-csv-path CONCEPT_CSV_PATH
                        Path to CSV of OMOP concept table.
  -r CONCEPT_RELATIONSHIP_CSV_PATH, --concept-relationship-csv-path CONCEPT_RELATIONSHIP_CSV_PATH
                        Path to CSV of OMOP concept_relationship table.
  -m {robot,yarrrml}, --method {robot,yarrrml}
                        What tooling / method to use to generate output?
  -v VOCABS [VOCABS ...], --vocabs VOCABS [VOCABS ...]
                        Used with `--output-type specific-vocabs-merged`. Which vocabularies to include
                        in the output? Usage: --vocabs "Procedure Type" "Device Type"
  -R RELATIONSHIPS [RELATIONSHIPS ...], --relationships RELATIONSHIPS [RELATIONSHIPS ...]
                        Which relationship types from the concept_relationship table's relationship_id
                        field to include? Default is "Subsumes" only. Passing "ALL" includes
                        everything. Ignored for --output-type options that are specific to a pre-set
                        vocabulary (e.g. rxnorm). Usage: --realationships "Subsumes" "Maps to"
  -S, --skip-semsql     In addition to .owl, also convert to a SemanticSQL .db? This is always True
                        except when --output-type is all-merged-post-split and it is creating initial
                        .owl files to be merged.
  -e, --exclude-singletons
                        Exclude terms that do not have any relationships. This only applies to --method
                        robot.
  -s, --semsql-only     Use this if the .owl already exists and you just want to create a SemanticSQL
                        .db.
  -C, --use-cache       Of outputs or intermediates already exist, use them.
```

