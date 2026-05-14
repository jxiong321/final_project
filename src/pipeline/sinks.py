import json
import csv

class JSONLSink:
    def __init__(self, path: str, headers: list):
        #store path
        self.path = path

        #open file for writing
            #store file handle on self
        self.file = open(self.path, 'w')

        self.headers = headers

        print("JSON Sink successfully created")
        
    def write(self, record):
        #take a dict, json.dumps it

        line = json.dumps(record)
        self.file.write(line)
        self.file.write("\n")

        print(f"wrote line {line}")

        #write to file followed by \n
    def close(self):
        #close the file handle
        self.file.close()
    
class CSVSink:
    def __init__(self, path: str, headers: list):
        self.path = path

        self.file = open(self.path, 'w')
        
        self.headers = headers
    
        self.writer = csv.DictWriter(self.file. self.headers)

        print("CSV Sink successfully created")
    
    def write(self, record):
        self.writer.writerow(record)

    def close(self):
        self.file.close()

#test
# if __name__ == "__main__":
#     sink = JSONLSink("test_output.jsonl")
#     sink.write({"name": "alice", "age": 30})
#     sink.write({"name": "bob", "age": 25})
#     sink.close()