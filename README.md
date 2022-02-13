# Hadoop-MapReduce-Calculating-Sales-by-Country

<p align="center">
  <img src="https://github.com/nikopetr/Hadoop-MapReduce-Anagram-Solver/blob/main/hadoop_img.png" width="700" height="300"/>
</p>

The implementation consists of a Java program that utilizes the Hadoop Map-Reduce framework for calculating the number of products and sales by country

**Author**: Nikolas Petrou, MSc in Data Science


## Task and Data 
Specifically this task focuses on finding the number of products and the sum of sales per country given the input file [SalesJan2009.csv](https://www.cs.ucy.ac.cy/courses/DSC511/data/SalesJan2009.csv)

<p align="center">
  <img src="https://github.com/nikopetr/Hadoop-MapReduce-Calculating-Sales-by-Country/blob/main/example_country_sales.png" width="750" height="350"/>
</p>

You can download & upload the aforementioned UNIX dictionary file to your own HDFS filesystem using the following commands:
- wget https://www.cs.ucy.ac.cy/courses/DSC511/data/SalesJan2009.csv
- hadoop fs -copyFromLocal SalesJan2009.csv filename_of_input_file


## Implementation
Output file example (part-r-00000):
- Argentina 1 1200
- Australia 38 64800
- Austria 7 10800

The main idea of this problem's solution is to use the same Key for every row with the same country name. In addition, the value used at each mapper will be the price (sales) of that row, which corresponds to the Key country.

In addition, since for this task we would like to output multiple values for each key, the code utilizes a custom made class that implements the [Writeable Interface](https://hadoop.apache.org/docs/r3.0.1/api/org/apache/hadoop/io/Writable.html). A custom hadoop writable data type which needs to be used as value field in Mapreduce programs must implement Writable interface org.apache.hadoop.io.Writable.

The desired output of the program is located in the [part-r-00000](https://github.com/nikopetr/Hadoop-MapReduce-Calculating-Sales-by-Country/blob/main/Sales/part-r-00000) file, while the code file is located in the [Sales.java](https://github.com/nikopetr/Hadoop-MapReduce-Calculating-Sales-by-Country/blob/main/Sales/src/Sales.java) file. There are more than enough comments which explain the whole implementation very analytically.


## Helpful Material-Links
If you are not very familiar with the Hadoop Map-Reduce framework, the following sites provide useful information for understanding some basic concepts, as well as some of the ideas of this task:

[Fundamentals of MapReduce with MapReduce Example](https://medium.com/edureka/mapreduce-tutorial-3d9535ddbe7c)

[Creating Custom Hadoop Writable Data Type](http://hadooptutorial.info/creating-custom-hadoop-writable-data-type/)

[MSc in Data Science Programme](https://datascience.cy/)
