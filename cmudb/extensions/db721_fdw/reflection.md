Strengths:
1) File format uses a columnar storage layout. This ensures that all the data for a column is located together and can be accessed efficiently for OLAP queries.
2) All blocks (except the last one) have the same number of attribute values and do not allow an attribute value to be split in multiple blocks. This makes extracting data easier. 
3) The format provides additional summary metadata for each block, which allows a faster way to filter out unneeded blocks.

Weaknesses: 
1) The data is currently stored in binary format without compression (RLE or dictionary for strings). This results in wasted space.
2) The data format imposes fixed length types and does not support variable length formats like precise integers or varchars.
3) The data is not sorted on any relevant key (or atleast provides no information about any existing sorting). Sorting will atleast make accesses on that key faster.

Suggestions for Improvement:
1) Use dictionary based encoding for strings to support varchars.
2) Sort data on some attribute and provide information of the sorted attribute in the json metadata.

Complexity: High (due to lack of documentation)
Difficulty: Medium (No difficult once you understand postres fdw)
Duration: 40-50hrs