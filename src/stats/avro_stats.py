"""
Given a local path to an avro file, print quick summary stats:
    total bytes
    # rows
    # blocks
    average row size in bytes
    average block size in bytes
    largest block size in bytes
 - usage: python <thisfile.py> <filepath>
 - uses fastavro library to read avro file
 - needs a local path (i.e. not in cloud shell)
 - sizes in bytes come from fastavro's block.size,
   sizes in BigQuery may be slightly larger due to overhead
"""

import sys
from fastavro import block_reader

def process_file(path):
    with open(path, 'rb') as file:
        reader = block_reader(file)
        total_bytes = 0
        num_blocks = 0
        num_rows = 0
        largest_block = 0
        for block in reader:
            for row in block:
                num_rows += 1
            num_blocks += 1
            if block.size > largest_block:
                largest_block = block.size
            total_bytes += block.size
    print(f'total bytes: {total_bytes:,} bytes')
    print(f'total rows: {num_rows:,}')
    print(f'total blocks: {num_blocks:,}')
    print(f'average row size: {total_bytes//num_rows:,} bytes')
    print(f'average block size: {total_bytes//num_blocks:,} bytes')
    print(f'largest block size: {largest_block:,} bytes')

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) != 2:
        print('Error reading arguments. Correct usage:')
        print('python <thisfile.py> <file.avro>')
    else:
        process_file(sys.argv[1]) # argv[1] must be the file path