"""Tests

Can run all tests in all files by running this from root of TermHub:
    python -m unittest discover
"""
import os
import sys
import unittest
from pathlib import Path
from typing import List, Set, Tuple, Union

import pandas as pd
from oaklib import BasicOntologyInterface, get_adapter
from oaklib.interfaces.basic_ontology_interface import RELATIONSHIP
from oaklib.types import CURIE, URI

TEST_DIR = Path(os.path.abspath(os.path.dirname(__file__)))
TEST_INPUT_DIR = TEST_DIR / 'input'
TEST_OUTPUT_DIR = TEST_DIR / 'output'
# TODO: Delete temp vars when not needed
TEMP_CONCEPT_CSV = '/Users/joeflack4/projects/TermHub/termhub-csets/datasets/prepped_files/concept.csv'
TEMP_CONCEPT_REL_CSV = '/Users/joeflack4/projects/TermHub/termhub-csets/datasets/prepped_files/concept_relationship.csv'
PROJECT_ROOT = TEST_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
from omop2owl_vocab import CONCEPT_DTYPES, CONCEPT_RELATIONSHIP_DTYPES, omop2owl


def _create_test_files(
    concept_csv_path: str = TEMP_CONCEPT_CSV, concept_relationship_csv_path: str = TEMP_CONCEPT_REL_CSV,
):
    """Create test files"""
    # Read inputs
    # - concept table
    concept_df = pd.read_csv(concept_csv_path, dtype=CONCEPT_DTYPES).fillna('')
    # todo: del index_col line
    # concept_df = pd.read_csv(concept_csv_path, index_col='concept_id', dtype=CONCEPT_DTYPES).fillna('')

    # - concept_relationship table
    concept_rel_df = pd.read_csv(concept_relationship_csv_path, dtype=CONCEPT_RELATIONSHIP_DTYPES).fillna('')
    concept_rel_df = concept_rel_df[concept_rel_df.invalid_reason == '']

    vocabs = ['ICD10CM', 'SNOMED', 'RxNorm', 'NDC', 'CPT4']  # arbitrary
    # vocabs = [x for x in list(concept_df['vocabulary_id'].unique()) if x]  # all
    for voc in vocabs:
        concept_df_i = concept_df[concept_df.vocabulary_id == voc]
        concept_df_i2 = concept_df_i.head(20)  # 20 = arbitrary
        concept_ids: Set[str] = set(concept_df_i2.concept_id)
        concept_rel_df_i = concept_rel_df[
            (concept_rel_df.concept_id_1.isin(concept_ids)) |
            (concept_rel_df.concept_id_2.isin(concept_ids))]
        this_test_dir = TEST_INPUT_DIR / voc
        os.makedirs(this_test_dir, exist_ok=True)
        concept_df_i2.to_csv(str(this_test_dir / 'concept.csv'), index=False)
        concept_rel_df_i.to_csv(str(this_test_dir / 'concept_relationship.csv'), index=False)

class TestOmop2Owl(unittest.TestCase):
    """Tests for database"""

    @staticmethod
    def _prep_combine_test_subsets(use_cache=True, create_fresh_test_files=False) -> Tuple[Path, Path]:
        """The tests are set up to be able to run snippets of vocabs, so as a pre-step, joins them."""
        # Vars
        outdir = TEST_OUTPUT_DIR / 'combined_inputs'
        concept_outpath = outdir / 'concept.csv'
        concept_rel_outpath = outdir / 'concept_relationship.csv'
        test_vocs = os.listdir(TEST_INPUT_DIR)

        # Create test files
        if create_fresh_test_files:
            _create_test_files()

        # Use cache
        if use_cache and os.path.exists(concept_outpath) and os.path.exists(concept_rel_outpath):
            return concept_outpath, concept_rel_outpath

        # Combine
        os.makedirs(outdir, exist_ok=True)
        concept_dfs: List[pd.DataFrame] = []
        concept_rel_dfs: List[pd.DataFrame] = []
        for test_voc in test_vocs:
            concept_dfs.append(pd.read_csv(TEST_INPUT_DIR / test_voc / 'concept.csv'))
            concept_rel_dfs.append(pd.read_csv(TEST_INPUT_DIR / test_voc / 'concept_relationship.csv'))
        concept_df = pd.concat(concept_dfs)
        concept_rel_df = pd.concat(concept_rel_dfs)

        # Save & return
        concept_df.to_csv(concept_outpath, index=False)
        concept_rel_df.to_csv(concept_rel_outpath, index=False)
        return concept_outpath, concept_rel_outpath

    def test_defaults_except_all_rels(self, use_cache=False):
        """Test default settings, except for including all relationships"""
        # Vars
        concept_outpath, concept_rel_outpath = self._prep_combine_test_subsets()
        outdir = TEST_OUTPUT_DIR / 'test_defaults'
        db_path = os.path.join(outdir, 'OMOP.db')
        settings = {
            'concept_csv_path': str(concept_outpath),
            'concept_relationship_csv_path': str(concept_rel_outpath),
            'outdir': str(outdir),
            'use_cache': use_cache,
            # Following 2 optoins are same as CLI's: 'output_type': 'merged-post-split',
            'split_by_vocab_merge_after': True,
            'split_by_vocab': True,
            # 'relationships': Default is: ['Subsumes'],
            'relationships': 'ALL',
            # More default values:
            # 'exclude_singletons': False,
            # 'memory': 100,
            # 'ontology_id': 'OMOP',
            # 'semsql_only': False,
            # 'skip_semsql': False,
            # 'vocabs': None,
        }

        # Run program
        omop2owl(**settings)

        # Tests
        oi: BasicOntologyInterface = get_adapter(db_path)
        ids: List[Union[CURIE, URI]] = [x for x in oi.entities(filter_obsoletes=False)]
        rels: List[RELATIONSHIP] = [x for x in oi.relationships(subjects=ids)]
        rel_set = set([x[1] for x in rels])
        self.assertGreater(len(ids), 100)
        self.assertGreater(len(rels), 50)
        self.assertIn('rdfs:subClassOf', rel_set)
        # self.assertGreater(len(rel_set), 1)  # reactivate this when bug fixed / clarified how to get all rels


# Special debugging: To debug in PyCharm and have it stop at point of error, change TestOmop2Owl(unittest.TestCase)
#  to TestOmop2Owl, and uncomment below.
# if __name__ == '__main__':
#     tester = TestOmop2Owl()
#     tester.test_defaults_except_all_rels()
