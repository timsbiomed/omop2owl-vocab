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
from datetime import datetime
from pathlib import Path
from string import Template
from typing import Dict, List, Union

import pandas as pd

PREFIX = str
CURIE = str
URI_STEM = str
SRC_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = SRC_DIR.parent
# ROBOT_PATH = 'robot'  # 2023/04/19: Strangely, this worked. Then, an hour later, only /usr/local/bin/robot worked
ROBOT_PATH = '/usr/local/bin/robot'
IO_DIR = PROJECT_DIR / 'io'
RELEASE_DIR = PROJECT_DIR / IO_DIR / 'release'
INPUT_DIR = PROJECT_DIR / IO_DIR / 'input'
TERMHUB_CSETS_DIR = INPUT_DIR / 'termhub-csets'
DATASETS_DIR = TERMHUB_CSETS_DIR / 'datasets' / 'prepped_files'
# CONCEPT_CSV = DATASETS_DIR / 'concept_temp.csv'  # todo: remove _temp when done w/ development (100 rows)
CONCEPT_CSV = DATASETS_DIR / 'concept.csv'
CONCEPT_CSV_RELPATH = str(CONCEPT_CSV).replace(str(IO_DIR), "")
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
OUTPATH_OWL = RELEASE_DIR / 'n3c.owl'
OUTPATH_NQUADS = RELEASE_DIR / 'n3c.nq'
OUTPATH_NQUADS_RELPATH = str(OUTPATH_NQUADS).replace(str(IO_DIR), "")
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


def via_robot_template(
    concept_subclass_ofs: Dict, num_subclass_cols: int, df: pd.DataFrame, outpath: Union[Path, str]
):
    """Create robot template and convert to OWL"""
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


def run_ingest(split_by_vocab: bool = False, skip_if_in_release: bool = True, method=['yarrrml', 'robot'][0]):
    """Run the ingest"""
    outpath = OUTPATH_OWL
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
    if split_by_vocab and method == 'robot':
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
                via_robot_template(concept_subclass_ofs, num_subclass_cols, group, outpath)
            except Exception:
                os.remove(outpath)
            t_i2 = datetime.now()
            print(f'Vocab {name} finished in {(t_i2 - t_i1).seconds} seconds')
            i += 1
    else:
        via_robot_template(concept_subclass_ofs, num_subclass_cols, concept_df, outpath)


if __name__ == '__main__':
    t1 = datetime.now()
    run_ingest()
    t2 = datetime.now()
    print(f'Finished in {(t2 - t1).seconds} seconds')
