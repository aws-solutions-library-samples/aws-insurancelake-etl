# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
import json
from urllib.parse import urlparse
import shutil

try:
    from test.glue_job_mocking_helper import *
    from awsglue.utils import GlueArgumentError
    import lib.glue_scripts.etl_collect_to_cleanse as etl_collect_to_cleanse
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')

mock_args = [
    'etl_collect-to-cleanse.py',
    '--JOB_NAME=UnitTest',
    '--JOB_RUN_ID=jr-unittest',
    f'--state_machine_name={mock_resource_prefix}-etl-state-machine',
    '--execution_id=test_execution_id',
    '--environment=unittest',
    f'--TempDir=file:///tmp/{mock_resource_prefix}-temp',
    f'--txn_bucket={mock_scripts_bucket}',
    '--txn_spec_prefix_path=/etl/transformation-spec/',
    f'--source_bucket={mock_collect_bucket}',
    f'--target_bucket=file:///tmp/{mock_resource_prefix}-cleanse-bucket',
    f'--source_path={mock_database_name}/{mock_table_name}',
    f'--source_key={mock_database_name}/{mock_table_name}',
    f'--target_database_name={mock_database_name}',
    f'--table_name={mock_table_name}',
    '--base_file_name=testfile.csv',
    '--p_year=2022',
    '--p_month=12',
    '--p_day=6',
    f'--hash_value_table={mock_resource_prefix}-hash-value-table',
    f'--value_lookup_table={mock_resource_prefix}-value-lookup-table',
    f'--multi_lookup_table={mock_resource_prefix}-multi-lookup-table',
    f'--dq_results_table={mock_resource_prefix}-dq-results-table',
]

mock_input_file_csv = """field1,field2
1,1/1/2022
2,12/31/2022
"""

mock_input_file_tsv = """field1\tfield2
1\t1/1/2022
2\t12/31/2022
"""

mock_mapping_file = """SourceName,DestName
field1,id
field2,date
"""

mock_spec_file = {}
mock_spec_file_tsv = {
    'input_spec': {
        'tsv': {}
    }
}


@mock_glue_job(etl_collect_to_cleanse)
def test_cleanse_bucket_write(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', mock_args)
    write_local_file(f'{mock_collect_bucket}/{mock_database_name}/{mock_table_name}', 'testfile.csv', mock_input_file_csv)

    cleanse_path = f'{mock_cleanse_bucket}/{mock_database_name}/{mock_table_name}'
    parsed_uri = urlparse(cleanse_path)
    shutil.rmtree(parsed_uri.path, ignore_errors=True)

    etl_collect_to_cleanse.main()
    assert os.path.isdir(parsed_uri.path), \
        'Collect-to-Cleanse Glue job failed to write to Cleanse bucket with supplied input file'


@mock_glue_job(etl_collect_to_cleanse)
def test_job_execution_and_commit_csv(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', mock_args)
    write_local_file(f'{mock_collect_bucket}/{mock_database_name}/{mock_table_name}', 'testfile.csv', mock_input_file_csv)
    write_local_file(f'{mock_scripts_bucket}/etl/transformation-spec', f'{mock_database_name}-{mock_table_name}.csv', mock_mapping_file)
    write_local_file(f'{mock_scripts_bucket}/etl/transformation-spec', f'{mock_database_name}-{mock_table_name}.json', json.dumps(mock_spec_file))
    etl_collect_to_cleanse.main()


@mock_glue_job(etl_collect_to_cleanse)
def test_job_execution_and_commit_tsv(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', mock_args)
    write_local_file(f'{mock_collect_bucket}/{mock_database_name}/{mock_table_name}', 'testfile.csv', mock_input_file_tsv)
    write_local_file(f'{mock_scripts_bucket}/etl/transformation-spec', f'{mock_database_name}-{mock_table_name}.csv', mock_mapping_file)
    write_local_file(f'{mock_scripts_bucket}/etl/transformation-spec', f'{mock_database_name}-{mock_table_name}.json', json.dumps(mock_spec_file_tsv))
    etl_collect_to_cleanse.main()


@mock_glue_job(etl_collect_to_cleanse)
def test_missing_argument_exception(monkeypatch, capsys):
    """
    Check error is raised if base_file_name argument is missing
    """
    mock_args_without_base_file_name = mock_args.copy()
    mock_args_without_base_file_name.remove('--base_file_name=testfile.csv')
    monkeypatch.setattr(sys, 'argv', mock_args_without_base_file_name)

    with pytest.raises(GlueArgumentError):
        etl_collect_to_cleanse.main()