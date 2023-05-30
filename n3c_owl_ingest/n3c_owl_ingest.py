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
 - Several questions sent to Ian Braun: https://obo-communitygroup.slack.com/archives/D056X9LUG4V/p1683673222343379
   - usage of omoprel
   - character set to allow for CURIEs (https://www.w3.org/TR/curie/#P_curie)
"""
import os
import subprocess
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from string import Template
from typing import Dict, List, Union

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
ROBOT_PATH = '/usr/local/bin/robot'
IO_DIR = PROJECT_DIR / 'io'
RELEASE_DIR = PROJECT_DIR / IO_DIR / 'release'
INPUT_DIR = PROJECT_DIR / IO_DIR / 'input'
PREFIXES_CSV = INPUT_DIR / 'prefixes.csv'
TERMHUB_CSETS_DIR = INPUT_DIR / 'termhub-csets'
DATASETS_DIR = TERMHUB_CSETS_DIR / 'datasets' / 'prepped_files'
# CONCEPT_CSV = DATASETS_DIR / 'concept_temp.csv'  # todo: remove _temp when done w/ development (100 rows)
CONCEPT_CSV = DATASETS_DIR / 'concept.csv'
CONCEPT_CSV_RELPATH = str(CONCEPT_CSV).replace(str(IO_DIR), "")
CONCEPT_RELATIONSHIP_CSV = DATASETS_DIR / 'concept_relationship.csv'
CONCEPT_RELATIONSHIP_SUBSUMES_CSV = DATASETS_DIR / 'concept_relationship_subsumes_only.csv'
CONCEPT_RELATIONSHIP_SUBSUMES_CSV_RELPATH = str(CONCEPT_RELATIONSHIP_SUBSUMES_CSV).replace(str(IO_DIR), "")
# - doesn't seem like concept_ancestor is necessary
# ancestor_concept_id,descendant_concept_id,min_levels_of_separation,max_levels_of_separation
# CONCEPT_ANCESTOR_CSV = DATASETS_DIR / 'concept_ancestor.csv'
YARRRML_TEMPLATE_FILENAME = "n3c_yarrrml.template.yaml"
YARRRML_TEMPLATE_PATH = INPUT_DIR / YARRRML_TEMPLATE_FILENAME
YARRRML_FILENAME = "n3c_yarrrml.yml"
# todo: consider storing in io/tmp/ instead
YARRRML_PATH = RELEASE_DIR / YARRRML_FILENAME
YARRRML_RELPATH = str(YARRRML_PATH).replace(str(IO_DIR), "")
RML_TEMPLATE_FILENAME = "n3c_rml.ttl"
# todo: consider storing in io/tmp/ instead
RML_TEMPLATE_PATH = RELEASE_DIR / RML_TEMPLATE_FILENAME
RML_TEMPLATE_RELPATH = str(RML_TEMPLATE_PATH).replace(str(IO_DIR), "")
ROBOT_TEMPLATE_PATH = RELEASE_DIR / 'n3c.robot.template.tsv'
OUTPATH_OWL = str(RELEASE_DIR / 'n3c.owl')
OUTPATH_NQUADS = RELEASE_DIR / 'n3c.nq'
OUTPATH_NQUADS_RELPATH = str(OUTPATH_NQUADS).replace(str(IO_DIR), "")
ONTOLOGY_IRI = 'http://purl.obolibrary.org/obo/N3C/ontology'
# PREFIX_MAP_STR = 'OMOP: http://purl.obolibrary.org/obo/N3C_'
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
REL_PRED_MAP = {
    'Subsumes': 'rdfs:subClassOf',
    'RxNorm inverse is a': 'rdfs:subClassOf',
}


def via_robot_template(
    df: pd.DataFrame, rel_maps: REL_MAPS, outpath: Union[Path, str], robot_subheader: Dict[str, str] = ROBOT_SUBHEADER,
    use_cache=False
):
    """Create robot template and convert to OWL"""
    # concepts_in_domain = set(df.index)
    outpath_template = str(outpath).replace('.owl', '.robot.template.tsv')
    # rdfs:subClassOf represented always as 'SC' in robot subheader, so handled separately
    robot_subheader = \
        robot_subheader | {k: f'A {k} SPLIT=|' for k in [x for x in rel_maps.keys() if x != 'rdfs:subClassOf']}

    if not(os.path.exists(outpath_template) and use_cache):
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

    if not(os.path.exists(outpath) and use_cache):
        # Convert to OWL
        print(f' - converting to OWL')
        command = f'export ROBOT_JAVA_ARGS=-Xmx28G; ' \
                  f'"{ROBOT_PATH}" template ' \
                  f'--template "{outpath_template}" ' \
                  f'--prefix "{PREFIX_MAP_STR}" ' \
                  f'--ontology-iri "{ONTOLOGY_IRI}" ' \
                  f'--output "{outpath}"'
        results = subprocess.run(command, capture_output=True, shell=True)
        print(results.stdout.decode())

    if not(os.path.exists(outpath.replace('.owl', '..db')) and use_cache):
        print(f' - converting to SemanticSQL')
        # todo: remove these comments when done
        # syntax: docker run -v $PWD:/work -w /work -ti linkml/semantic-sql semsql make foo.db
        # example: RUST_BACKTRACE=full semsql make $@ -P config/prefixes.csv
        # TODO: replace with just 'docker' when fixed. see: https://youtrack.jetbrains.com/issue/PY-45462/Pycharm-environment-variable-Path-is-different-from-python-console
        # TODO: add back "-P {PREFIXES_CSV" when fixed: Usage: semsql make [OPTIONS] PATH\nTry 'semsql make --help' for help.\n\nError: No such option: -P\n"
        rel_outpath = outpath.replace(str(IO_DIR), "")
        command = f'/usr/local/bin/docker run ' \
                  f'-v {IO_DIR}:/work ' \
                  f'-w /work ' \
                  f'linkml/semantic-sql ' \
                  f'semsql make /work/{rel_outpath.replace(".owl", ".db")}'
        # f'semsql make {outpath.replace(".owl", ".db")} -P {PREFIXES_CSV}'
        results = subprocess.run(command, capture_output=True, shell=True)
        print(results.stdout.decode())
        return results


def via_yarrrml(retain_intermediates=True, use_cache=True):
    """Create YARRML yaml and convert to OWL"""
    # todo: consider:
    # capture_output = True, shell = True
    # print(results1.stdout.decode())

    # Load the yaml template and populate it
    # /data: the directory in the docker container that is mapped to the IO_DIR on the host
    substitution_map = {
        "concept_table_filename": "/data/" + CONCEPT_CSV_RELPATH,
        "concept_relationship_table_filename": "/data/" + CONCEPT_RELATIONSHIP_SUBSUMES_CSV_RELPATH
    }
    with open(YARRRML_TEMPLATE_PATH, "r") as f:
        in_contents = f.read()
        out_contents = Template(in_contents).safe_substitute(substitution_map)
    with open(YARRRML_PATH, "w") as f:
        f.write(out_contents)

    # Convert this temp YARRRML mapping template to a RML mapping template.
    subprocess.run([
        "docker", "run", "--rm",
        "-v", f"{IO_DIR}:/data",
        "-e", "JAVA_TOOL_OPTIONS=\"-Xmx28G\"", "--memory", "28g",
        "rmlio/yarrrml-parser:latest",
        "-i", f"/data/{YARRRML_RELPATH}",
        "-o", f"/data/{RML_TEMPLATE_RELPATH}"
    ])
    print('Step 1/3: Done: RML template.')

    # Use the RML mapping template to process the csv's and output the triples as a ttl file.
    # - https://github.com/RMLio/rmlmapper-java
    # - Output format: nquads (default) https://www.w3.org/TR/n-quads/
    if not (use_cache and Path(OUTPATH_NQUADS).exists()):
        subprocess.run([
            "docker", "run", "--rm",
            "-v", f"{IO_DIR}:/data",
            "-e", "JAVA_TOOL_OPTIONS=\"-Xmx28G\"", "--memory", "28g",
            "rmlio/rmlmapper-java:v5.0.0",
            "-m", f"/data/{RML_TEMPLATE_RELPATH}",
            "-o", f"/data/{OUTPATH_NQUADS_RELPATH}"
        ])
    print('Step 2/3: Done: Conversion from RML mapping template to nquads complete.')

    # TODO: implement final conversion step
    print('Step 3/3: Converting nquads to OWL using Apache Jena\'s RIOT. TODO: Not yet implemented.')
    # todo: I have no idea if I need to set this, and if so, whether or not to use export, and what variable to use
    # https://jena.apache.org/documentation/io/#
    # memory_opts = 'MY_JAVA_OPTS="-Xmx26GB"; JAVA_OPTIONS="-Xmx26GB"; JAVA_OPTS="-Xmx26GB"; _JAVA_OPTIONS="-Xmx26GB"; ' \
    #               'JAVA_TOOL_OPTIONS="-Xmx26GB"; export MY_JAVA_OPTS="-Xmx26GB"; export JAVA_OPTIONS="-Xmx26GB"; ' \
    #               'export JAVA_OPTS="-Xmx26GB"; export _JAVA_OPTIONS="-Xmx26GB"; export JAVA_TOOL_OPTIONS="-Xmx26GB";'
    # subprocess.run(memory_opts.split(' ') + [
    # subprocess.run([
    #     "riot", "--output=rdfxml", "-v", OUTPATH_NQUADS, ">", OUTPATH_OWL
    # ])
    # todo: not sure why it'd not running:
    # 17:39:01 INFO  riot            :: File: /Users/joeflack4/projects/n3c-owl-ingest/io/release/n3c.nq
    # 17:41:20 INFO  riot            :: File: >
    # 17:41:20 ERROR riot            :: Not found: file:///Users/joeflack4/projects/n3c-owl-ingest/%3E
    # org.apache.jena.riot.RiotException: Not found: file:///Users/joeflack4/projects/n3c-owl-ingest/%3E
    # 	at org.apache.jena.riot.system.ErrorHandlerFactory$ErrorHandlerTracking.error(ErrorHandlerFactory.java:317)
    # 	at riotcmd.CmdLangParse.parseRIOT(CmdLangParse.java:309)
    # 	at riotcmd.CmdLangParse.parseFile(CmdLangParse.java:259)
    # 	at riotcmd.CmdLangParse.exec$(CmdLangParse.java:165)
    # 	at riotcmd.CmdLangParse.exec(CmdLangParse.java:130)
    # 	at org.apache.jena.cmd.CmdMain.mainMethod(CmdMain.java:87)
    # 	at org.apache.jena.cmd.CmdMain.mainRun(CmdMain.java:56)
    # 	at org.apache.jena.cmd.CmdMain.mainRun(CmdMain.java:43)
    # 	at riotcmd.riot.main(riot.java:29)

    # echo TODO: Convert OWL to SemSQL
    #

    # Cleaning up temporary files and copying back out of the working directory.
    if not retain_intermediates:
        os.remove(YARRRML_TEMPLATE_PATH)
        os.remove(RML_TEMPLATE_PATH)
    return


def main_ingest(
    split_by_vocab: bool = False, concept_csv_path: str = CONCEPT_CSV,
    concept_relationship_csv_path: str = CONCEPT_RELATIONSHIP_SUBSUMES_CSV, skip_if_in_release: bool = True,
    vocabs: List[str] = [], relationships: List[str] = ['Subsumes'], method=['yarrrml', 'robot'][0], use_cache=False
):
    """Run the ingest"""
    os.makedirs(RELEASE_DIR, exist_ok=True)
    # todo: excessive customization for rxnorm here is code smell. what if rxnorm + atc situation changes?
    outpath = OUTPATH_OWL if not vocabs \
        else OUTPATH_OWL.replace('n3c.owl', 'n3c-RxNorm.owl') if 'RxNorm' in vocabs and len(vocabs) < 3 \
        else OUTPATH_OWL.replace('n3c.owl', f'n3c-{"-".join(vocabs)}.owl')
    if skip_if_in_release and os.path.exists(outpath):
        print('Skipping because already exists:', outpath)
        return
    if split_by_vocab and method == 'yarrrml':
        raise NotImplemented('Not implemented yet: Splitting using YARRRML method.')
    elif method == 'yarrrml':
        return via_yarrrml()
    # else: method = 'robot'

    # Read inputs
    # - concept_relationship table
    concept_rel_df = pd.read_csv(concept_relationship_csv_path, dtype=CONCEPT_RELATIONSHIP_DTYPES).fillna('')
    concept_rel_df = concept_rel_df[concept_rel_df.invalid_reason == '']
    # todo: include automatic addition of these relationships?
    # if vocabs:
    #     rels = concept_rel_df.relationship_id.unique()
    # if 'RxNorm' in vocabs:
    #     # noinspection PyUnboundLocalVariable pycharm_wrong
    #     relationships += [x for x in rels if 'rx' in x.lower()]
    # if 'ATC' in vocabs:
    #     # noinspection PyUnboundLocalVariable pycharm_wrong
    #     relationships += [x for x in rels if 'atc' in x.lower()]
    # relationships = list(set(relationships))
    rel_maps: REL_MAPS = {}
    for rel in relationships:
        pred: PREDICATE_ID = f'omoprel:{rel.replace(" ", "_")}' if rel not in REL_PRED_MAP else REL_PRED_MAP[rel]
        rel_maps[pred] = {}
        df_i = concept_rel_df[concept_rel_df.relationship_id == rel]
        for row in df_i.itertuples(index=False):
            # noinspection PyUnresolvedReferences It_doesnt_know_that_row_is_a_namedtuple
            rel_maps[pred].setdefault(row.concept_id_2, []).append(row.concept_id_1)

    # - concept table
    concept_df = pd.read_csv(concept_csv_path, index_col='concept_id', dtype=CONCEPT_DTYPES).fillna('')

    # Construct robot template
    # - Convert concept table to robot template format
    if vocabs and method == 'robot':
        df = concept_df[concept_df.vocabulary_id.isin(vocabs)]
        via_robot_template(df, rel_maps, outpath, use_cache=use_cache)
    elif split_by_vocab and method == 'robot':
        grouped = concept_df.groupby('vocabulary_id')
        i = 1
        name: str
        for name, group_df in grouped:
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
                via_robot_template(group_df, rel_maps, outpath, use_cache=use_cache)
            except Exception:
                os.remove(outpath)
            t_i2 = datetime.now()
            print(f'Vocab {name} finished in {(t_i2 - t_i1).seconds} seconds')
            i += 1
    else:
        via_robot_template(concept_df, rel_maps, outpath, use_cache=use_cache)


def cli():
    """Command line interface."""
    parser = ArgumentParser('Creates TSVs and of unmapped terms as well as summary statistics.')
    parser.add_argument(
        '-o', '--output-type', required=False, default='all-merged', choices=['all-merged', 'all-split', 'rxnorm'],
        help='What output to generate? If "all-merged" will create an n3c.db file with all concepts of all vocabs '
             'merged into one. If "all-split" will create an n3c-*.db file for each vocab. If "rxnorm" will create a '
             'specifically customized n3c-RxNorm.db.')
    parser.add_argument(
        '-c', '--concept-csv-path', required=False, default=CONCEPT_CSV, help='Path to CSV of OMOP concept table.')
    parser.add_argument(
        '-r', '--concept-relationship-csv-path', required=False, default=CONCEPT_RELATIONSHIP_SUBSUMES_CSV,
        help='Path to CSV of OMOP concept_relationship table.')
    parser.add_argument(
        '-m', '--method', required=False, default='yarrrml', choices=['robot', 'yarrrml'],
        help='What tooling / method to use to generate output?')
    parser.add_argument(
        '-C', '--use-cache', required=False, action='store_true',
        help='Of outputs or intermediates already exist, use them')
    d = vars(parser.parse_args())
    if d['output_type'] == 'all-split':
        main_ingest(
            split_by_vocab=True, concept_csv_path=d['concept_csv_path'],
            concept_relationship_csv_path=d['concept_relationship_csv_path'], use_cache=d['use_cache'])
    elif d['output_type'] == 'all-merged':
        if d['concept_csv_path'] != CONCEPT_CSV or d['concept_relationship_csv_path'] != CONCEPT_RELATIONSHIP_SUBSUMES_CSV:
            raise ValueError('Not implemented yet: Custom concept CSVs with all-merged output.')
        main_ingest(split_by_vocab=False, use_cache=d['use_cache'])
    elif d['output_type'] == 'rxnorm':
        # rxnorm_ingest(concept_csv_path=d['concept_csv_path'], concept_relationship_csv_path=d['concept_relationship_csv_path'])
        main_ingest(
            split_by_vocab=True, method='robot', vocabs=['RxNorm', 'ATC'],
            relationships=['Subsumes', 'Maps to', 'RxNorm inverse is a'],
            concept_csv_path=d['concept_csv_path'], concept_relationship_csv_path=d['concept_relationship_csv_path'],
            use_cache=d['use_cache'])


if __name__ == '__main__':
    t1 = datetime.now()
    cli()
    t2 = datetime.now()
    print(f'Finished in {(t2 - t1).seconds} seconds')
