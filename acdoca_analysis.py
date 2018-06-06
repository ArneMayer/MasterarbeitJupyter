import pandas as pd
import numpy as np
import math
import random

df = pd.read_csv('/mnt/data/tmp_arne_ma/data/acdoca/acdoca1M.csv')
column_count = len(df.columns)
row_count = len(df)
chunk_size = 100000
sample_size = 10
chunk_count = int(math.ceil(row_count / float(chunk_size)))

print("Row count: " + str(row_count))
print("Chunk size: " + str(chunk_size))
print("Chunk count:" + str(chunk_count))

for column_id in range(0, column_count):
    pruned_chunks = 0
    for n in range(0, sample_size):
        scan_value = df.iloc[random.randint(0,row_count - 1)][column_id]
        for chunk_id in range(0, chunk_count):
            chunk_prunable = True
            for chunk_offset in range(0, chunk_size):
                table_offset = chunk_id * chunk_size + chunk_offset
                if table_offset >= row_count:
                    break
                value = df.iloc[table_offset][column_id]
                if value == scan_value:
                    chunk_prunable = False
            if chunk_prunable:
                pruned_chunks += 1

    pruning_rate = pruned_chunks / float(chunk_count * sample_size)
    print("Pruning rate for column {}: {}".format(column_id, pruning_rate))
