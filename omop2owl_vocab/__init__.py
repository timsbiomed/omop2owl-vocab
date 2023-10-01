"""N3C OMOP to OWL"""
from datetime import datetime

from omop2owl_vocab.omop2owl_vocab import cli, CONCEPT_DTYPES, CONCEPT_RELATIONSHIP_DTYPES, run


if __name__ == '__main__':
    t1 = datetime.now()
    cli()
    t2 = datetime.now()
    print(f'Finished in {(t2 - t1).seconds} seconds')
