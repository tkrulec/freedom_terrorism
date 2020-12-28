hdfs dfs -rm -r /user/jk5804/project/project_code/
hdfs dfs -mkdir /user/jk5804/project/project_code

#if files already exist, remove files
hdfs dfs -rm /user/jk5804/project/project_code/CleanMapper_gtd.py
hdfs dfs -rm /user/jk5804/project/project_code/CleanMapper_hfi.py
hdfs dfs -rm /user/jk5804/project/project_code/CleanReducer_gtd.py
hdfs dfs -rm /user/jk5804/project/project_code/CleanReducer_hfi.py

#put files into the code folders
hdfs dfs -put CleanMapper_gtd.py /user/jk5804/project/project_code
hdfs dfs -put CleanMapper_hfi.py /user/jk5804/project/project_code
hdfs dfs -put CleanReducer_gtd.py /user/jk5804/project/project_code
hdfs dfs -put CleanReducer_hfi.py /user/jk5804/project/project_code


hdfs dfs -chmod a+x project/project_code/*.py

#if folder already exists, remove output folder
hdfs dfs -rm -r /user/jk5804/project/gtd
hdfs dfs -rm -r /user/jk5804/project/hfi

rm gtd_cleaned.txt
rm hfi_cleaned.txt

# #mapreduce
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/jk5804/project/project_code/CleanMapper_gtd.py,hdfs://dumbo/user/jk5804/project/project_code/CleanReducer_gtd.py -mapper "python CleanMapper_gtd.py" -reducer "python CleanReducer_gtd.py" -input /user/jk5804/project/gtd_data.csv -output /user/jk5804/project/gtd
hdfs dfs -cat /user/jk5804/project/gtd/part-00000> gtd_cleaned.txt
hdfs dfs -put gtd_cleaned.txt

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/jk5804/project/project_code/CleanMapper_hfi.py,hdfs://dumbo/user/jk5804/project/project_code/CleanReducer_hfi.py -mapper "python CleanMapper_hfi.py" -reducer "python CleanReducer_hfi.py" -input /user/jk5804/project/hfi_data.csv -output /user/jk5804/project/hfi
hdfs dfs -cat /user/jk5804/project/hfi/part-00000> hfi_cleaned.txt
hdfs dfs -put hfi_cleaned.txt
# hdfs dfs -put hfi_cleaned_2.txt #> for some of the analytics run

#Tina
#hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/kk3506/project/python_code/cleaning-hfi/CleanMapper_hfi.py,hdfs://dumbo/user/kk3506/project/python_code/cleaning-hfi/CleanReducer_hfi.py -mapper "python CleanMapper_hfi.py" -reducer "python CleanReducer_hfi.py" -input /user/kk3506/project/hfi_data.csv -output /user/kk3506/project/output/cleaning-hfi
#hdfs dfs -cat /user/kk3506/project/output/cleaning-hfi/part-00000> hfi_cleaned.txt
