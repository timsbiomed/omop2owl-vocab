"""N3C OMOP to OWL"""
from datetime import datetime

from omop2owl_vocab.omop2owl_vocab import cli


if __name__ == '__main__':
    t1 = datetime.now()
    cli()
    t2 = datetime.now()
    print(f'Finished in {(t2 - t1).seconds} seconds')
