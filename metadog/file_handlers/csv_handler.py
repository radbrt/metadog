import csv
from metadog.json_schema import generate_schema
import io

class CSVHandler():
    def __init__(self, file_stream, file_name, get_schema=False, delimiter=None):
        self.filestream = io.TextIOWrapper(file_stream, encoding='utf-8')
        self.get_schema = get_schema
        self.delimiter = delimiter or self.get_delimiter()
        self.accepted_file_types = ['csv']

        self.file_name = file_name

    def get_file_metadata(self):
        if self.has_header() and self.get_schema:   

            t, samples = self.sample_file(sample_rate=100, max_records=1000)
            print(samples)
            schema = generate_schema(samples)
        else:
            schema = {}

        return {'file': self.file_name, 'properties': schema}



    def has_header(self):
        csv_test_bytes = self.filestream.read(5000)
        sniffer = csv.Sniffer()
        has_header = sniffer.has_header(csv_test_bytes)
        self.filestream.seek(0)
        print(f"Header? {has_header}")
        return has_header


    def get_delimiter(self):
        csv_test_bytes = self.filestream.read(5000)
        sniffer = csv.Sniffer()
        delimiter = sniffer.sniff(csv_test_bytes).delimiter
        self.filestream.seek(0)

        return delimiter
    

    def sample_file(self, sample_rate=100, max_records=1000):
        fullfile = self.filestream.readlines()
        csvfile = csv.DictReader(fullfile, fieldnames=None, delimiter=self.delimiter)
        samples = []


        current_row = 0
        for row in csvfile:
            if (current_row % sample_rate) == 0:
                samples.append(row)

            current_row += 1

            if len(samples) >= max_records:
                break

        # Empty sample to show field selection, if needed
     # Empty sample to show field selection, if needed
        empty_file = False
        if csvfile.fieldnames is None:
            empty_file = True
            samples.append({})
        else:
            samples.append({name: None for name in csvfile.fieldnames})

        self.filestream.seek(0)
        return (empty_file, samples)

