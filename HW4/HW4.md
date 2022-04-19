Problem 1

1. read in each file in input directory to a rdd of (word, file_number) and union those file rdds

2. create new rdd of (word, file_number) frequency => ((word, file_number), frequency) and reformat to (word, string(file_number, frequency))

3. reformat rdd and write to output

4. run jar file with 

spark-submit --class spark.zjx.invertedIndex target/spark.jar

5. output is 

![image info](p1.png)
