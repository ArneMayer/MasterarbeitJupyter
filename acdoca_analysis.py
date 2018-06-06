import pandas as pd
import numpy as np

df = pd.read_csv('/mnt/data/tmp_arne_ma/data/acdoca/acdoca10M.csv')
column_count = len(df.columns)
row_count = len(df)
chunk_size = 100000
sample_size = 10
chunk_count = ceil(row_count / chunk_size)

print("Row count: " + row_count)
print("Chunk size: " + chunk_size)

for column_id in range(0, column_count):
    pruned_chunks = 0
    for n in range(0, sample_size):
        scan_value = df[column_id].iloc[random.randint(0,row_count - 1)]
        for chunk_id in range(0, chunk_count):
            chunk_prunable = True
            for chunk_offset in range(0, chunk_size):
                table_offset = chunk_id * chunk_size + chunk_offset
                if table_offset >= row_count:
                    break
                value = df[column_id].iloc[value]
                if value == scan_value:
                    chunk_prunable = False
            if chunk_prunable:
                pruned_chunks += 1

    pruning_rate = pruned_chunks / float(chunk_count * sample_size)
    print("Pruning rate for column {}: {}".format(column_id, pruning_rate))
