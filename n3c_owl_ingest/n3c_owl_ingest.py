"""N3C OMOP to OWL"""
import os
from pathlib import Path
from typing import Dict

import pandas as pd

CURIE = str
SRC_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = SRC_DIR.parent
TERMHUB_CSETS_DIR = PROJECT_DIR / 'termhub-csets'
DATASETS_DIR = TERMHUB_CSETS_DIR / 'datasets' / 'prepped_files'
CONCEPT_CSV = DATASETS_DIR / 'concept.csv'
CONCEPT_RELATIONSHIP_CSV = DATASETS_DIR / 'concept_relationship_subsumes_only.csv'
# - doesn't seem like concept_ancestor is necessary
# ancestor_concept_id,descendant_concept_id,min_levels_of_separation,max_levels_of_separation
# CONCEPT_ANCESTOR_CSV = DATASETS_DIR / 'concept_ancestor.csv'


def run_ingest():
    """Run the ingest"""
    # concept: header & example row
    # concept_id,concept_name,domain_id,vocabulary_id,concept_class_id,standard_concept,concept_code,valid_start_date,valid_end_date,invalid_reason
    # 22945,Horizontal overbite,Condition,SNOMED,Clinical Finding,S,70305005,2002-01-31,2099-12-31,
    concept_df = pd.read_csv(CONCEPT_CSV)

    # todo: use concept_relationship after constructing omop_d &/or native_d
    # concept: header & example row
    # concept_id_1,concept_id_2,relationship_id,valid_start_date,valid_end_date,invalid_reason
    # 260759,22945,Subsumes,2011-07-31,2099-12-31,
    concept_relationship_df = pd.read_csv(CONCEPT_RELATIONSHIP_CSV)

    omop_d: Dict[CURIE, Dict[str, str]] = {}
    native_d: Dict[CURIE, Dict[str, str]] = {}
    for _, row in concept_df.iterrows():
        # todo: ignore some concept_class_ids?
        id_omop = row['concept_id']
        id_native = row['concept_code']
        label = row['concept_name']
        vocab_name = row['concept_class_id']
        prefix = vocab_name  # TODO: from using vocab_name and looking up a prefix_map
        curie_omop = f'N3C:{id_omop}'  # todo: if we stick with this, I need a purl for N3C
        curie_native = f'{prefix}:{id_native}'
        omop_d[curie_omop] = {
            'ID': curie_omop,
            'Label': label,
            'Type': 'class',
            'Parent Class': '',
        }
        native_d[curie_native] = {
            'ID': curie_native,
            'Label': label,
            'Type': 'class',
            'Parent Class': '',
        }

    # TODO: concept_relationship only has omop_id's, so if I want to classify by native ID, need to do lil extra work
    for _, row in concept_df.iterrows():
        pass

    # TODO: construct a robot template
    # http://robot.obolibrary.org/template
    # Example TSV (w/ only fields that I think we'll need; 2nd row is `robot` header):
    # ID	Label	Type	Parent Class
    # ID	A rdfs:label	TYPE	SC %
    # ex:F344N	F 344/N	class	NCBITaxon:10116
    rows = list(omop_d.values()) + list(native_d.values())
    robot_df = pd.DataFrame(rows)
    # todo: then prepend the robot row
    pass


if __name__ == '__main__':
    run_ingest()
