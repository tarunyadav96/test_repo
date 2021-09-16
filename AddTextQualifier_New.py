import pandas as pd
from sys import argv
import os
from datetime import datetime
import logging

input_path = ""
output_path = ""
delimiter = ""
delete_source = 0

if len(argv) > 1:
    input_path = argv[1]
if len(argv) > 2:
    output_path = argv[2]
if len(argv) > 3:
    delimiter = argv[3]
if len(argv) > 4:
    delete_source = argv[4]

if input_path == "":
    input_path = "D:\\Raw Data\\FinancialModelling\\TEST"
if output_path == "":
    output_path = "D:\\Raw Data\\FinancialModelling\\Output_test"
if delimiter == "":
    delimiter = ","

import csv
def findDelimiter():
    sniffer = csv.Sniffer()
    for root, subdirs, files in os.walk(input_path):
        for filename in files:
            with open (root + "\\" + filename, "r") as myfile:
                data = myfile.readline()
            dialect = sniffer.sniff(data)
            delim= dialect.delimiter
            return delim

readdelim = findDelimiter()
if len(delimiter) != 0:
    writedelim = delimiter
else:
    writedelim = readdelim

for root, dirs, filenames in os.walk(input_path):
    for filename in filenames:
        try:
            print("Processing " + root+"\\" + filename)
            folderstructure = root.replace(input_path,output_path)
            if not os.path.exists(folderstructure):
                os.makedirs(folderstructure)
            print(folderstructure + "\\" + filename)
            with open(root + "\\" + filename,'r') as src:
                df = pd.read_csv(src,header = 0,encoding = "ISO-8859-1",
                                 sep = readdelim,dtype = object, chunksize = 100)
                write_header = True
                write_mode = 'w'
                for chunk in df:
                    chunk = chunk.dropna(how = 'all')
                    cols = list(chunk.columns.values)
                    cols = [col.replace(",","") for col in cols]
                    chunk.columns = cols
                    chunk.to_csv(folderstructure + "\\" + filename, index = False,
                                 sep = writedelim, quotechar = "\"",quoting = csv.QUOTE_ALL,
                                 encoding = "ISO-8859-1",mode = write_mode,header = write_header)
                    write_header = False
                    write_mode = 'a'
                df = pd.read_csv(folderstructure + "\\" + filename, sep = writedelim, encoding = "ISO-8859-1")
                df = df.dropna(axis=1,how='all')
                cols = list(df.columns)
                new_cols = [name for name in cols if name.strip()]
                df[new_cols].to_csv(folderstructure + "\\" + filename, index = False,
                                 sep = writedelim, quotechar = "\"",quoting = csv.QUOTE_ALL,
                                 encoding = "ISO-8859-1")
            if int(delete_source) == 1:
                try:
                    os.remove(root + "\\" + filename)
                except OSError:
                    pass
        except Exception as e:
            print(e)
