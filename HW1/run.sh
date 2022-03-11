hadoop fs -rm -r zhangj/output_dir
rm -r mapreduce/*
javac -cp $(hadoop classpath) HW1_mapreduce.java -d mapreduce/
jar -cvf HW1_mapreduce.jar -C mapreduce/ .
hadoop jar HW1_mapreduce.jar HW1_mapreduce zhangj/input_dir/HTTP_20130313143750.dat zhangj/output_dir
