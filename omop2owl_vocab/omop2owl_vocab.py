"""OMOP to OWL

TODO's
 - Include a robot.jar and instead add Java to prereqs in README
 - concept_relationship
   - add valid_start_date & valid_end_date fields? how? axiom annotations?
 - concept
   - ignore some concept_class_ids? Such as if not SNOMED, etc
 - concept_ancestor
   - May want to use this. See workflowy / comments here
 - Several questions sent to Ian Braun: https://obo-communitygroup.slack.com/archives/D056X9LUG4V/p1683673222343379
   - usage of omoprel
   - character set to allow for CURIEs (https://www.w3.org/TR/curie/#P_curie)
"""
import hashlib
import os
import pickle
import shutil
import subprocess
import sys
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple, Union

import pandas as pd

PREFIX = str
CURIE = str
URI_STEM = str
CONCEPT_ID = int
PREDICATE_ID = str
REL_MAPS = Dict[PREDICATE_ID, Dict[CONCEPT_ID, List[CONCEPT_ID]]]
SRC_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = SRC_DIR.parent
# ROBOT_PATH = 'robot'  # 2023/04/19: Strangely, this worked. Then, an hour later, only /usr/local/bin/robot worked
# ROBOT_PATH = '/usr/local/bin/robot'
# DOCKER_PATH = '/usr/local/bin/docker'
ROBOT_PATH = 'robot'
DOCKER_PATH = 'docker'
PREFIXES_CSV = SRC_DIR / 'prefixes.csv'
PREFIX_MAP = {
    'omoprel': 'https://w3id.org/cpont/omop/relations/',
    'OMOP': 'https://athena.ohdsi.org/search-terms/terms/',
}
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
    'domain_id': 'A OMOP:domain_id',
    'vocabulary_id': 'A OMOP:vocabulary_id',
    'concept_class_id': 'A OMOP:concept_class_id',
    'standard_concept': 'A OMOP:standard_concept',
    'concept_code': 'A OMOP:concept_code',
    'valid_start_date': 'A OMOP:valid_start_date',
    'valid_end_date': 'A OMOP:valid_end_date',
    'invalid_reason': 'A OMOP:invalid_reason',
    'rdfs:subClassOf': 'SC % SPLIT=|',
}
# REL_PRED_MAPPINGS: This is where we want to convert the OMOP relationship to a common predicate
# - REL_PRED_REVERSE_MAPPING: For some of these, we actually can infer an inverse predicate and want to use that
# instead. In these cases, the subject and object order will be flipped so that the directionality of the relationship
# is corrected.
# todo: though duplicative, add option to keep originals as well, e.g. keep both "Is a" and "rdfs:subClassOf"
REL_PRED_MAPPING = {
    'Is a': 'rdfs:subClassOf',
}
REL_PRED_REVERSE_MAPPING = {
    'RxNorm inverse is a': 'rdfs:subClassOf',
}
REL_PRED_MAPPINGS = REL_PRED_MAPPING | REL_PRED_REVERSE_MAPPING
# ROBOT_PREFIX_ERR_REPLACEMENTS: This is a hack because of some current issues with robot. I'm trying to use explicit
# prefixes 'omoprel' and 'OMOP', but robot is ignoring my prefixes and just looking at the URI stem, and taking the last
# part after the last / to be the prefix, e.g. 'relations' and 'terms'."
ROBOT_PREFIX_ERR_REPLACEMENTS = {
    'relations': 'omoprel',
    'terms': 'OMOP',
}
CONFIG = {
    'semsql_show_stacktrace': ['full', 'lite', 'none'][0]
}
PROG = 'omop2owl-vocab'
DESC = 'Convert OMOP vocabularies to OWL and SemanticSQL.'


def _run_command(command: str):
    results = subprocess.run(command, capture_output=True, shell=True)
    out = str(results.stdout.decode()).strip()
    err = str(results.stderr.decode()).strip()
    if out:
        print(out)
    if err:
        print(err, file=sys.stderr)
    return out, err


def _convert_semsql(owl_outpath: str, quiet=False, memory: int = 100):
    """Convert to SemanticSQL"""
    if not quiet:
        print(f' - converting to SemanticSQL')
    # todo: ideal if backtrace worked: ex: RUST_BACKTRACE=full semsql make $@ -P config/prefixes.csv
    #  docker: Error response from daemon: failed to create shim task: OCI runtime create failed: runc create failed:
    #  unable to start container process: exec: "RUST_BACKTRACE=full": executable file not found in $PATH: unknown.
    # todo: replace with just 'docker' when fixed. see: https://youtrack.jetbrains.com/issue/PY-45462/Pycharm-environment-variable-Path-is-different-from-python-console
    prefixes_path = 'prefixes.csv'
    outdir = os.path.dirname(owl_outpath)
    outfile = os.path.basename(owl_outpath).replace(".owl", ".db")
    shutil.copy(PREFIXES_CSV, os.path.dirname(owl_outpath))
    # Run
    err_conf = CONFIG['semsql_show_stacktrace']
    stacktrace_str = '' if err_conf == 'none' else 'RUST_BACKTRACE=1 ' if err_conf == 'lite' else 'RUST_BACKTRACE=full '
    command = f'{DOCKER_PATH} run ' \
              f'-v {outdir}:/work ' \
              f"-e ROBOT_JAVA_ARGS='-Xmx{str(memory)}G' " \
              f'-w /work ' \
              f'obolibrary/odkfull:dev ' \
              f'{stacktrace_str}semsql -v make ' \
              f'{outfile} ' \
              f'-P {prefixes_path}'
    out, err = _run_command(command)
    # todo: I'm not sure why this error is happening when using omop2owl as a packge only
    # docker: Error response from daemon: failed to create shim task: OCI runtime create failed: runc create failed:
    # unable to start container process: exec: "RUST_BACKTRACE=full": executable file not found in $PATH: unknown.
    if err and f'unable to start container process: exec: "{stacktrace_str.strip()}"' in err:
        _run_command(command.replace(stacktrace_str, ''))

    # Cleanup
    intermediate_patterns = ['.db.tmp', '-relation-graph.tsv.gz']
    for pattern in intermediate_patterns:
        path = os.path.join(os.path.dirname(owl_outpath), os.path.basename(owl_outpath).replace('.owl', pattern))
        if os.path.exists(path):
            os.remove(path)
    _cleanup_leftover_semsql_intermediates(os.path.dirname(owl_outpath))


# TODO: also clean up copied over prefixes.csv?
def _cleanup_leftover_semsql_intermediates(_dir):
    """Cleanup leftover intermediate files created by SemanticSQL"""
    semsql_template_path = os.path.join(_dir, '.template.db')
    if os.path.exists(semsql_template_path):
        os.remove(semsql_template_path)


def _get_merged_file_outpath(outdir: str, ontology_id: str, vocabs: List[str]) -> str:
    """Get outpath of merged ontology
    todo: excessive customization for rxnorm here is code smell. what if rxnorm + atc situation changes?"""
    out_filename = f'{ontology_id}.owl'
    outpath_owl = os.path.join(outdir, out_filename)
    outpath = outpath_owl if not vocabs \
        else outpath_owl.replace(out_filename, f'{ontology_id}-RxNorm.owl') if 'RxNorm' in vocabs and len(vocabs) < 3 \
        else outpath_owl.replace(out_filename, f'{ontology_id}-{"-".join(vocabs)}.owl')
    return outpath


def _create_outputs(
    df: pd.DataFrame, rel_maps: REL_MAPS, outpath: Union[Path, str], ontology_iri: str,
    robot_subheader: Dict[str, str] = ROBOT_SUBHEADER, use_cache=False, skip_semsql=False, memory: int = 100,
    do_fixes=True
) -> bool:
    """Create robot template and convert to OWL and SemanticSQL
    :returns Whether or not using cached version of OWL"""
    # todo: remove this replacement when taken care of properly elsewhere
    outpath = os.path.join(os.path.dirname(outpath), os.path.basename(outpath).replace(' ', '-'))
    # concepts_in_domain = set(df.index)
    outpath_template = str(outpath).replace('.owl', '.robot.template.tsv')
    # rdfs:subClassOf represented always as 'SC' in robot subheader, so handled separately
    robot_subheader = \
        robot_subheader | {k: f'A {k} SPLIT=|' for k in [x for x in rel_maps.keys() if x != 'rdfs:subClassOf']}

    if not(os.path.exists(outpath_template) and use_cache):
        print(f' - creating robot template')
        d: Dict[CURIE, Dict[str, str]] = {}
        for row in df.itertuples():
            # todo: faster if I build curies beforehand
            # noinspection PyUnresolvedReferences It_doesnt_know_that_row_is_a_namedtuple
            curie_omop = f'OMOP:{row.Index}'
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
                'rdfs:subClassOf': '',
            }
            # todo: faster if jagged? e.g. each entry has actual amount its parents & let pd.DataFrame(d.values()) handle?
            for rel, rel_map_i in rel_maps.items():
                try:
                    # noinspection PyUnresolvedReferences It_doesnt_know_that_row_is_a_namedtuple
                    concept_ids: List[int] = rel_map_i[row.Index]
                    # if parent_concept_id not in concepts_in_domain:
                    #     print(f'Warning: Parent not in vocab. This causes issue w/ our strategy to split table.: {vocab_id}', file=sys.stderr)
                    row_dict[rel] = '|'.join([f'OMOP:{x}' for x in concept_ids])
                except (KeyError, IndexError):
                    row_dict[rel] = ''
            d[curie_omop] = row_dict

        # - Create CSV
        robot_df = pd.DataFrame([robot_subheader] + list(d.values()))
        robot_df.to_csv(outpath_template, index=False, sep='\t')

    using_cached_owl: bool = os.path.exists(outpath) and use_cache
    if not using_cached_owl:
        # Convert to OWL
        print(f' - converting to OWL')
        command = \
            f'export ROBOT_JAVA_ARGS=-Xmx{str(memory)}G; ' \
            f'"{ROBOT_PATH}" template ' \
            f'--template "{outpath_template}" ' \
            f'--ontology-iri "{ontology_iri}" ' \
            f'--output "{outpath}"'
        for k, v in PREFIX_MAP.items():
            command += f' --prefix "{k}: {v}"'
        _run_command(command)

    if do_fixes:
        # Fix issue w/ robot not accepting --prefix'es
        with open(outpath, 'r') as f:
            contents = f.read()
        for k, v in ROBOT_PREFIX_ERR_REPLACEMENTS.items():
            contents = contents.replace(f'<{k}:', f'<{v}:')  # opening tags
            contents = contents.replace(f'</{k}:', f'</{v}:')  # closing tags
            contents = contents.replace(f'xmlns:{k}', f'xmlns:{v}')  # header
        with open(outpath, 'w') as f:
            f.write(contents)

    if not(os.path.exists(str(outpath).replace('.owl', '.db')) and use_cache) and not skip_semsql:
        _convert_semsql(outpath)

    return using_cached_owl


def _get_header_body_footer(file_str: str) -> Tuple[str, str, str]:
    """From an RDF/XML OWL serialization string, extract header and body as 2 string objects
    Code created largely through: https://chat.openai.com/share/2c4c8eb7-c7a5-496e-add3-c6f5687e04eb"""
    import re
    heaader_pattern = r'^[\s\S]*?ontology"\/>'
    header_match = re.search(heaader_pattern, file_str)
    header = header_match.group(0)

    footer_pattern = r'</rdf:RDF>([\s\S]*)'
    footer_match = re.search(footer_pattern, file_str)
    footer = footer_match.group(0)

    split_text = re.split(heaader_pattern, file_str, maxsplit=1)
    if len(split_text) == 2:
        body = split_text[1]
        body = body.replace(footer, '')
    else:
        raise RuntimeError('Had a problem joining each separate vocabulary into a single OWL. Header regex issue.')

    return header, body, footer


def _get_relationship_maps(concept_rel_df: pd.DataFrame, relationships: List[str], concept_ids: Set[str]) -> REL_MAPS:
    """Get relationship maps"""
    concept_rel_df = concept_rel_df.sort_values(['relationship_id'])
    rel_maps: REL_MAPS = {}
    rels = relationships if relationships != ['ALL'] else concept_rel_df.relationship_id.unique()
    # XML namespace encoding. See: https://github.com/HOT-Ecosystem/omop2owl/issues/10
    # - allowed: : _ - .
    sanitized_rel_map = {
        x: x.replace(" ", "_")
        .replace("\t", "_")
        .replace("\n", "_")
        .replace(",", "_")
        .replace("|", "_")
        .replace(";", "_")
        .replace("/", ".")
        .replace("\\", ".")
        .replace("~", "-")
        .replace("`", "-")
        .replace("!", "-")
        .replace("@", "-")
        .replace("#", "-")
        .replace("$", "-")
        .replace("%", "-")
        .replace("^", "-")
        .replace("*", "-")
        .replace("+", "-")
        .replace("=", "-")
        .replace("?", "-")
        .replace("'", "-")
        .replace('"', "-")
        .replace("(", "-")
        .replace(")", "-")
        .replace("[", "-")
        .replace("]", "-")
        .replace("{", "-")
        .replace("}", "-")
        .replace("<", "-")
        .replace(">", "-")
        for x in rels
    }
    # todo: any way to increase performance?
    for i, rel_mapping in enumerate(sanitized_rel_map.items()):
        rel, sanitized_rel = rel_mapping
        print(f' - {i + 1} of {len(rels)}: {rel}')
        reverse_rel, remapped_rel = rel in REL_PRED_REVERSE_MAPPING, rel in REL_PRED_MAPPING
        pred: PREDICATE_ID = REL_PRED_REVERSE_MAPPING[rel] if reverse_rel else REL_PRED_MAPPINGS[rel] if remapped_rel\
            else f'omoprel:{sanitized_rel}'
        rel_maps[pred] = {}
        # pred: PREDICATE_ID = f'omoprel:{rel}' if rel not in REL_PRED_MAP else REL_PRED_MAP[rel]
        df_i = concept_rel_df[concept_rel_df.relationship_id == rel]
        df_i = df_i[df_i['concept_id_1'].isin(concept_ids)]

        # todo: any way to increase performance below?
        # rel_maps[pred]: chatgpt thought this was faster but in 1 experiemnt it was 3x slower
        # rel_maps[pred] = df_i.groupby('concept_id_2')['concept_id_1'].apply(list).to_dict()
        for row in df_i.itertuples(index=False):
            if reverse_rel:
                rel_maps[pred].setdefault(row.concept_id_2, []).append(row.concept_id_1)
            else:
                rel_maps[pred].setdefault(row.concept_id_1, []).append(row.concept_id_2)
    return rel_maps


def _get_core_objects(
    concept_csv_path: str, concept_relationship_csv_path: str, outpath: str, vocabs: List[str] = [], relationships: List[str] = ['Is a'],
    exclude_singletons: bool = False, use_cache=False
) -> Tuple[pd.DataFrame, REL_MAPS]:
    """Get core objects"""
    t_0 = datetime.now()
    # Load cache
    cache_filename = os.path.basename(outpath).replace(".owl", "") + (
        f'__vocabs_{"_".join(vocabs)}' if vocabs else '') + f'__relationships_{"_".join(relationships)}'
    cache_hash = hashlib.md5(cache_filename.encode('utf-8')).hexdigest()
    cache_path = os.path.join(os.path.dirname(outpath), cache_hash + '.pkl')
    if use_cache and os.path.exists(cache_path):
        with open(cache_path, 'rb') as f:
            t_1 = datetime.now()
            d = pickle.load(f)
            print('Loaded cached tables and objects in', (t_1 - t_0).seconds, 'seconds')
            return d['concept_df'], d['rel_maps']

    # Read inputs
    # - concept table
    concept_df = pd.read_csv(concept_csv_path, index_col='concept_id', dtype=CONCEPT_DTYPES).fillna('')
    concept_ids: Set[str] = set(concept_df.index)
    t_1 = datetime.now()
    print('Read "concept" table in', (t_1 - t_0).seconds, 'seconds')

    # - concept_relationship table
    concept_rel_df = pd.read_csv(concept_relationship_csv_path, dtype=CONCEPT_RELATIONSHIP_DTYPES).fillna('')
    concept_rel_df = concept_rel_df[concept_rel_df.invalid_reason == '']
    t_2 = datetime.now()
    print('Read "concept_relationships" table in', (t_2 - t_1).seconds, 'seconds')

    # Filter by vocab
    if vocabs:
        concept_df = concept_df[concept_df.vocabulary_id.isin(vocabs)]
        concept_ids: Set[str] = set(concept_df.index)
        concept_rel_df = concept_rel_df[
            (concept_rel_df.concept_id_1.isin(concept_ids)) |
            (concept_rel_df.concept_id_2.isin(concept_ids))]
        t_3 = datetime.now()
        print('Filtered vocabularies in', (t_3 - t_2).seconds, 'seconds')
    # todo: include automatic addition of these relationships in specific vocabs?
    # if 'RxNorm' in vocabs:
    #     relationships += [x for x in rels if 'rx' in x.lower()]
    # if 'ATC' in vocabs:
    #     relationships += [x for x in rels if 'atc' in x.lower()]

    # Group relationships
    t_4a = datetime.now()
    print('Grouping relationships...')
    rel_maps: REL_MAPS = _get_relationship_maps(concept_rel_df, relationships, concept_ids)
    t_4b = datetime.now()
    print('Grouped relationships in', (t_4b - t_4a).seconds, 'seconds')

    # Filter out singletons
    if exclude_singletons:
        concepts_with_relations = set(concept_rel_df.concept_id_1) | set(concept_rel_df.concept_id_2)
        concept_df = concept_df[~concept_df.index.isin(concepts_with_relations)]

    # Cache and return
    with open(cache_path, 'wb') as f:
        d = {'concept_df': concept_df, 'rel_maps': rel_maps}
        pickle.dump(d, f, protocol=pickle.HIGHEST_PROTOCOL)
    return concept_df, rel_maps


def run(
    concept_csv_path: str, concept_relationship_csv_path: str, split_by_vocab: bool = False,
    split_by_vocab_merge_after: bool = False, vocabs: List[str] = [],
    relationships: List[str] = ['Is a'], use_cache=False, skip_semsql: bool = False,
    exclude_singletons: bool = False, memory: int = 100,
    ontology_id: str = 'OMOP',  # add str(randint(100000, 999999))?
    outdir: str = os.getcwd()  # or RELEASE_DIR?
):
    """Run the ingest"""
    # Basic setup
    _cleanup_leftover_semsql_intermediates(outdir)
    outdir = outdir if os.path.isabs(outdir) else os.path.join(os.getcwd(), outdir)
    os.makedirs(outdir, exist_ok=True)
    outpath: str = _get_merged_file_outpath(outdir, ontology_id, vocabs)
    ontology_iri = f'http://purl.obolibrary.org/obo/{ontology_id}/ontology'
    if isinstance(vocabs, str):
        vocabs = [vocabs]
    if isinstance(relationships, str):
        relationships = [relationships]
    # -  SemSQL errors if space in name
    outpath = os.path.join(os.path.dirname(outpath), os.path.basename(outpath).replace(' ', '-'))
    if use_cache and os.path.exists(outpath.replace('.owl', '.db')):
        print('Skipping because of --use-cache. Already exists:', outpath)
        return

    # Run
    concept_df, rel_maps = _get_core_objects(
        concept_csv_path, concept_relationship_csv_path, outpath, vocabs, relationships, exclude_singletons, use_cache)
    if vocabs or not split_by_vocab:
        return _create_outputs(
            concept_df, rel_maps, outpath, ontology_iri, use_cache=use_cache, skip_semsql=skip_semsql, memory=memory)

    # - Split by vocab
    # -- Create outputs by vocab
    # todo: put in its own func
    grouped = concept_df.groupby('vocabulary_id')
    i = 1
    name: str
    vocab_outpaths: List[Path] = []
    uncached_owl_exists = False
    for name, group_df in grouped:
        name = name if name else 'Metadata'  # AFAIK, there's just 1 concept "No matching concept" for this
        t_i1 = datetime.now()
        print(f'Creating outputs {i} of {len(grouped)}: {name}')
        vocab_outpath = Path(outdir) / f'{name}.owl'.replace(' ', '-')
        vocab_outpaths.append(vocab_outpath)
        # noinspection PyBroadException
        try:
            ontology_iri_i = f'http://purl.obolibrary.org/obo/{name}/ontology'
            # todo: The way this is, it makes it maybe look like there is an option in the CLI to allow the user to
            #  include semsql output when doing all-merged-post-split, but that's not the case.
            using_cached_owl = _create_outputs(
                group_df, rel_maps, vocab_outpath, ontology_iri_i, use_cache=use_cache, memory=memory,
                skip_semsql=True if split_by_vocab_merge_after else skip_semsql)
            if not using_cached_owl:
                uncached_owl_exists = True
        except Exception:
            os.remove(vocab_outpath)
        t_i2 = datetime.now()
        print(f' - finished in {(t_i2 - t_i1).seconds} seconds\n')
        i += 1

    # -- Merge outputs by vocab
    # todo: put in its own func
    # todo: group annotation props & classes together
    #  - right now the annotation props will get duplicated, and the comment headers for these will also get duplicated.
    #  - classes should be unique though
    if split_by_vocab_merge_after and (not (os.path.exists(outpath) and use_cache) or uncached_owl_exists):
        if os.path.exists(outpath):
            os.remove(outpath)
        print(f'Joining vocab .owl files into a single OWL: {outpath}')
        with open(outpath, 'a') as file:
            for i, path in enumerate(vocab_outpaths):
                print(f' - {i + 1} of {len(vocab_outpaths)}: {os.path.basename(path).replace(".owl", "")}')
                with open(path) as vocab_file:
                    try:
                        original_contents = vocab_file.read()
                        header, body, footer = _get_header_body_footer(original_contents)
                        # Header: Do 1x at beginning
                        if i == 0:
                            file.write(header)
                        # Body
                        file.write(body)
                        # Footer: Do 1x at end
                        if i == len(vocab_outpaths) - 1:
                            file.write(footer)
                    except Exception as e:
                        os.remove(outpath)
                        raise e
    if not (os.path.exists(outpath.replace('.owl', '.db')) and use_cache):
        print(f'Converting to SemanticSQL')
        _convert_semsql(outpath, quiet=True, memory=memory)


def cli(title: str = PROG, description: str = DESC):
    """Command line interface."""
    parser = ArgumentParser(prog=title, description=description)
    # Required
    parser.add_argument(
        '-c', '--concept-csv-path', required=False, help='Path to CSV of OMOP concept table.')
    parser.add_argument(
        '-r', '--concept-relationship-csv-path', required=False,
        help='Path to CSV of OMOP concept_relationship table.')
    # Optional
    parser.add_argument(
        '-O', '--outdir', required=False, default=os.getcwd(), help='Output directory.')
    # todo:would be good to allow them to pass their own pURL
    parser.add_argument(
        '-I', '--ontology-id', required=False, default='OMOP',  # add str(randint(100000, 999999))?
        help='Identifier for ontology. Used to generate a pURL and file name.')
    parser.add_argument(
        '-o', '--output-type', required=False, default='merged-post-split',
        choices=['merged', 'split', 'merged-post-split', 'rxnorm'],
        help='What output to generate? If "merged" will create an ONTOLOGY_ID.db file with all concepts of all vocabs '
             'merged into one. If "split" will create an ONTOLOGY_ID-*.db file for each vocab. "merged-post-split" '
             'output will be as if running both "split" and  "merged", but the merging implementation is different. '
             'Use this option if running out of memory. If using "rxnorm", will create a specifically customized '
             'ONTOLOGY_ID-RxNorm.db.')
    parser.add_argument(
        '-v', '--vocabs', required=False, nargs='+',
        help='Used with `--output-type specific-vocabs-merged`. Which vocabularies to include in the output?  Usage: '
             '--vocabs "Procedure Type" "Device Type"')
    parser.add_argument(
        '-R', '--relationships', required=False, nargs='+', default=['Is a'],
        help='Which relationship types from the concept_relationship table\'s relationship_id field to include? '
             'Default is "Is a" only. Passing "ALL" includes everything. Ignored for --output-type options that are '
             'specific to a pre-set vocabulary (e.g. rxnorm). Usage: --realationships "Is a" "Maps to"')
    parser.add_argument(
        '-S', '--skip-semsql', required=False, action='store_true',
        help='In addition to .owl, also convert to a SemanticSQL .db? This is always True except when --output-type is '
             'all-merged-post-split and it is creating initial .owl files to be merged.')
    parser.add_argument(
        '-e', '--exclude-singletons', required=False, action='store_true',
        help='Exclude terms that do not have any relationships. This only applies to --method robot.')
    parser.add_argument(
        '-s', '--semsql-only', required=False, action='store_true',
        help='Use this if the .owl already exists and you just want to create a SemanticSQL .db.')
    parser.add_argument(
        '-C', '--use-cache', required=False, action='store_true',
        help='Of outputs or intermediates already exist, use them.')
    parser.add_argument(
        '-M', '--memory', required=False, default=100, help='The amount of Java memory (GB) to allocate.')
    parser.add_argument('-i', '--install', action='store_true', help='Installs necessary docker images.')

    # TODO: Need to switch to **kwargs for most of below
    d = vars(parser.parse_args())
    if d['install']:
        _run_command('docker pull obolibrary/odkfull:dev')
        print('Installation complete. Exiting.')
        return
    if not d['concept_csv_path'] or not d['concept_relationship_csv_path']:
        raise RuntimeError('Must pass --concept-csv-path and --concept-relationship-csv-path')
    if d['semsql_only']:
        outpath: str = _get_merged_file_outpath(d['outdir'], d['ontology_id'], d['vocabs'])
        _convert_semsql(outpath, memory=d['memory'])
    elif d['output_type'] == 'split':
        run(
            concept_csv_path=d['concept_csv_path'], concept_relationship_csv_path=d['concept_relationship_csv_path'],
            split_by_vocab=True, use_cache=d['use_cache'], skip_semsql=d['skip_semsql'],
            exclude_singletons=d['exclude_singletons'], relationships=d['relationships'], vocabs=d['vocabs'],
            memory=d['memory'], outdir=d['outdir'])
    elif d['output_type'] == 'merged-post-split':  # Default
        run(
            concept_csv_path=d['concept_csv_path'], concept_relationship_csv_path=d['concept_relationship_csv_path'],
            split_by_vocab=True, split_by_vocab_merge_after=True, use_cache=d['use_cache'],
            skip_semsql=d['skip_semsql'], exclude_singletons=d['exclude_singletons'], relationships=d['relationships'],
            vocabs=d['vocabs'], memory=d['memory'], outdir=d['outdir'])
    elif d['output_type'] == 'merged':
        run(
            concept_csv_path=d['concept_csv_path'], concept_relationship_csv_path=d['concept_relationship_csv_path'],
            split_by_vocab=False, use_cache=d['use_cache'], skip_semsql=d['skip_semsql'], memory=d['memory'],
            exclude_singletons=d['exclude_singletons'], relationships=d['relationships'], vocabs=d['vocabs'],
            outdir=d['outdir'])
    elif d['output_type'] == 'rxnorm':
        # rxnorm_ingest(concept_csv_path=d['concept_csv_path'], concept_relationship_csv_path=d['concept_relationship_csv_path'])
        run(
            concept_csv_path=d['concept_csv_path'], concept_relationship_csv_path=d['concept_relationship_csv_path'],
            split_by_vocab=True, vocabs=['RxNorm', 'ATC'], use_cache=d['use_cache'],
            relationships=['Is a', 'Maps to', 'RxNorm inverse is a'], skip_semsql=d['skip_semsql'],
            exclude_singletons=d['exclude_singletons'], memory=d['memory'], outdir=d['outdir'])


if __name__ == '__main__':
    t1 = datetime.now()
    cli()
    t2 = datetime.now()
    print(f'Finished in {(t2 - t1).seconds} seconds')
