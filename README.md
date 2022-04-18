<div id="top"></div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#execution">Execution</a>
    </li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

Program performs join (inner, left and right) operation on two csv files

<p align="right">(<a href="#top">back to top</a>)</p>


## Built With

Program built in Python 3.8


<p align="right">(<a href="#top">back to top</a>)</p>

## Describtion

Program uses two partitioning methods so as not to read whole files into memory

- internal partitioning

Program reads data from base file in blocks. Each block is transformed into a dictionary that maps header value to list of rows. That makes pairing header values with corresponding rows very fast. Also, instead of traversing a whole joined file for each row of the base file in search of relations, we do that only for each of the blocks and the linkage is checked very fast in dictionary. Size of read block can be specified by an option -batch.


- external partitioning

Second optimization technique is used if we set a -hash option to a value greater then 1. It partitions provided files into smaller temporary ones thus reducing number of future traversed rows. By using hashing on a chosen header we can partition data itself into smaller groups. Rows with chosen headers of the same value will be stored
together removing the risk of losing potential relations. This will be effective for larger files.

all generated rows are outputted to standard input

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- EXECUTION -->
## Execution

Execution:
```
join.py -h [-hash HASH] [-batch BATCH] operation base_file_path joined_file_path column_name [join_type]
```
Tests:
```
python -m unittest discover
```

positional attributes:
```
- operation - operation to be performed (currently only join is supported)
- base_file_path - path to a base file
- joined_file_path - path to a joined file
- column_name - column name by which we join files
- join_type (optional) - available types are: [inner, left, right]
```
options:
```
- -h - for help
- -hash - specifies number of external file partitions (default=1)
  setting value higher than 1 will result in creation of temporary files
- -batch - specifies number of internal file partitions (default=1000)
```
example:

We would like to left join file test0.csv with test1.csv by column "id".
We will supply optional arguments with default values for presentation sake
```
python join.py join ./test/test_data/test0.csv ./test/test_data/test1.csv id left -hash 1 -batch 1000
```
