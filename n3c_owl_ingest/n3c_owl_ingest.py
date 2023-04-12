"""N3C OMOP to OWL

todo: temp: other considerations
 - alternatives to 'N3C' fields: JSON dump to (a) rdf:description, (b) definition
 http://purl.obolibrary.org/obo/IAO_0000115

TODO's
 - Include a robot.jar and instead add Java to prereqs in README
 - concept_relationship
   - add valid_start_date & valid_end_date fields? how? axiom annotations?
 - concept
   - ignore some concept_class_ids? Such as if not SNOMED, etc
 - concept_ancestor
   - May want to use this. See workflowy / comments here
"""
import os
import subprocess
# import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd

CURIE = str
SRC_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = SRC_DIR.parent
# ROBOT_PATH = 'robot'  # 2023/04/19: Strangely, this worked. Then, an hour later, only /usr/local/bin/robot worked
ROBOT_PATH = '/usr/local/bin/robot'
RELEASE_DIR = PROJECT_DIR / 'release'
TERMHUB_CSETS_DIR = PROJECT_DIR / 'termhub-csets'
DATASETS_DIR = TERMHUB_CSETS_DIR / 'datasets' / 'prepped_files'
# CONCEPT_CSV = DATASETS_DIR / 'concept_temp.csv'  # todo: remove _temp when done w/ development (100 rows)
CONCEPT_CSV = DATASETS_DIR / 'concept.csv'
CONCEPT_RELATIONSHIP_SUBSUMES_CSV = DATASETS_DIR / 'concept_relationship_subsumes_only.csv'
# - doesn't seem like concept_ancestor is necessary
# ancestor_concept_id,descendant_concept_id,min_levels_of_separation,max_levels_of_separation
# CONCEPT_ANCESTOR_CSV = DATASETS_DIR / 'concept_ancestor.csv'
OUTPATH_TEMPLATE = RELEASE_DIR / 'n3c.robot.template.tsv'
OUTPATH_OWL = RELEASE_DIR / 'n3c.owl'
ONTOLOGY_IRI = 'http://purl.obolibrary.org/obo/N3C/ontology'
# PREFIX_MAP_STR = 'N3C: http://purl.obolibrary.org/obo/N3C_'
PREFIX_MAP_STR = 'OMOP: https://athena.ohdsi.org/search-terms/terms/'
CONCEPT_DTYPES = {
    'concept_id': str,  # is int, but we're just serializing, not manipulating
    'concept_name': str,
    'domain_id': str,
    'vocabulary_id': str,
    'concept_class_id': str,
    'standard_concept': str,
    'concept_code': str,
    'valid_start_date': str,  # is date, but we're just serializing, not manipulating
    'valid_end_date': str,  # is date, but we're just serializing, not manipulating
    'invalid_reason': str
}
CONCEPT_RELATIONSHIP_DTYPES = {
    'concept_id_1': str,  # is int, but we're just serializing, not manipulating
    'concept_id_2': str,  # is int, but we're just serializing, not manipulating
    'relationship_id': str,
    'valid_start_date': str,  # is date, but we're just serializing, not manipulating
    'valid_end_date': str,  # is date, but we're just serializing, not manipulating
    'invalid_reason': str,
}
ROBOT_SUBHEADER = {
    'ID': 'ID',
    'Label': 'A rdfs:label',
    'Type': 'TYPE',
    'domain_id': 'A N3C:domain_id',
    'vocabulary_id': 'A N3C:vocabulary_id',
    'concept_class_id': 'A N3C:concept_class_id',
    'standard_concept': 'A N3C:standard_concept',
    'concept_code': 'A N3C:concept_code',
    'valid_start_date': 'A N3C:valid_start_date',
    'valid_end_date': 'A N3C:valid_end_date',
    'invalid_reason': 'A N3C:invalid_reason',
}


def create_robot_template(
    concept_subclass_ofs: Dict, num_subclass_cols: int, df: pd.DataFrame, vocab_id: str = '',
    outpath: Union[Path, str] = OUTPATH_OWL
):
    """Create robot template"""
    outpath_template = str(outpath).replace('.owl', '.robot.template.tsv')
    # concepts_in_domain = set(df.index)

    d: Dict[CURIE, Dict[str, str]] = {}
    for row in df.itertuples():
        # todo: faster if I build curies beforehand
        # noinspection PyUnresolvedReferences It_doesnt_know_that_row_is_a_namedtuple
        curie_omop = f'N3C:{row.Index}'
        # noinspection PyUnresolvedReferences It_doesnt_know_that_row_is_a_namedtuple
        row_dict = {
            'ID': curie_omop,
            'Label': row.concept_name,
            'Type': 'class',
            'domain_id': row.domain_id,
            'vocabulary_id': row.vocabulary_id,
            'concept_class_id': row.concept_class_id,
            'standard_concept': row.standard_concept,
            'concept_code': row.concept_code,
            'valid_start_date': row.valid_start_date,
            'valid_end_date': row.valid_end_date,
            'invalid_reason': row.invalid_reason,
        }
        # todo: faster if jagged? e.g. each entry has actual amount its parents & let pd.DataFrame(d.values()) handle?
        for i in range(num_subclass_cols):
            try:
                # noinspection PyUnresolvedReferences It_doesnt_know_that_row_is_a_namedtuple
                parent_concept_id = concept_subclass_ofs[row.Index][i]
                # if parent_concept_id not in concepts_in_domain:
                #     print(f'Warning: Parent not in vocab. This causes issue w/ our strategy to split table.: {vocab_id}', file=sys.stderr)
                parent_curie_omop = f'N3C:{parent_concept_id}'
                row_dict[f'Parent Class {i + 1}'] = parent_curie_omop
            except (KeyError, IndexError):
                row_dict[f'Parent Class {i + 1}'] = ''
        d[curie_omop] = row_dict

    # - Add robot subheader
    for i in range(num_subclass_cols):
        ROBOT_SUBHEADER[f'Parent Class {i + 1}'] = f'SC %'
    # - Create CSV
    robot_df = pd.DataFrame([ROBOT_SUBHEADER] + list(d.values()))
    robot_df.to_csv(outpath_template, index=False, sep='\t')

    # Convert to OWL
    command = f'export ROBOT_JAVA_ARGS=-Xmx28G; ' \
              f'"{ROBOT_PATH}" template ' \
              f'--template "{outpath_template}" ' \
              f'--prefix "{PREFIX_MAP_STR}" ' \
              f'--ontology-iri "{ONTOLOGY_IRI}" ' \
              f'--output "{outpath}"'
    results = subprocess.run(command, capture_output=True, shell=True)
    print(results.stdout.decode())
    return results


def run_ingest(split_by_vocab: bool = False, skip_if_in_release: bool = True):
    """Run the ingest"""
    # Read inputs
    # - concept_relationship table
    concept_subsumes_df = pd.read_csv(CONCEPT_RELATIONSHIP_SUBSUMES_CSV, dtype=CONCEPT_RELATIONSHIP_DTYPES).fillna('')
    concept_subsumes_df = concept_subsumes_df[concept_subsumes_df.invalid_reason == '']
    concept_subclass_ofs: Dict[List[int]] = {}
    for row in concept_subsumes_df.itertuples(index=False):
        # noinspection PyUnresolvedReferences It_doesnt_know_that_row_is_a_namedtuple
        concept_subclass_ofs.setdefault(row.concept_id_2, []).append(row.concept_id_1)
    # how_many_parents_gt_1 = [len(x) > 1 for x in concept_subclass_ofs.values()].count(True)  # analysis: 215,489 / ~7m
    num_subclass_cols = max([len(x) for x in concept_subclass_ofs.values()])  # determined by greatest num parents
    # - concept table
    concept_df = pd.read_csv(CONCEPT_CSV, index_col='concept_id', dtype=CONCEPT_DTYPES).fillna('')

    # Construct robot template
    # - Convert concept table to robot template format
    if split_by_vocab:
        grouped = concept_df.groupby('vocabulary_id')
        i = 1
        name: str
        for name, group in grouped:
            name = name if name else 'Metadata'  # AFAIK, there's just 1 concept "No matching concept" for this
            t_i1 = datetime.now()
            print('Starting vocab', i, 'of', len(grouped), ':', name)
            outfile = f'n3c-{name}.owl'
            outpath = RELEASE_DIR / outfile
            if skip_if_in_release and os.path.exists(outpath):
                i += 1
                continue
            # noinspection PyBroadException
            try:
                create_robot_template(concept_subclass_ofs, num_subclass_cols, group, name, outpath)
            except Exception:
                os.remove(outpath)
            t_i2 = datetime.now()
            print(f'Vocab {name} finished in {(t_i2 - t_i1).seconds} seconds')
            i += 1
    else:
        create_robot_template(concept_subclass_ofs, num_subclass_cols, concept_df)


if __name__ == '__main__':
    t1 = datetime.now()
    run_ingest()
    t2 = datetime.now()
    print(f'Finished in {(t2 - t1).seconds} seconds')
