from sqlalchemy import create_engine, inspect
import fsspec
import csv
from .json_schema import sample_file, generate_schema



def profile_files(protocol, filetype='csv', get_schema=True, n_samples=1000, **kwargs):
    fs = fsspec.filesystem(protocol, **kwargs)
    fl = fs.ls('./')
    csvs = [f for f in fl if f.endswith(f'.{filetype}')]

    schemas = []
    for file in csvs:
        with fs.open(file, 'r') as fin:
            csv_test_bytes = fin.read(5000)
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(csv_test_bytes)
            if has_header and get_schema:   
                delimiter = sniffer.sniff(csv_test_bytes).delimiter
                fin.seek(0)
                fullfile = fin.readlines()
                csvfile = csv.DictReader(fullfile, fieldnames=None, delimiter=delimiter)
                t, samples = sample_file(csvfile, has_header=has_header, sep=delimiter, sample_rate=100, max_records=1000)
                schema = generate_schema(samples)
            else:
                schema = None
        schemas.append({'file': file, 'properties': schema})

    return schemas


def get_file_list(protocol, **kwargs):
    fs = fsspec.filesystem(protocol, **kwargs)
    return fs.ls('./')

