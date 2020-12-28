# create a project folder on Dumbo
-mkdir project
# create a project folder on HDFS
hdfs dfs -mkdir project

# Export of data from local to Dumbo
scp ./gtd_data.csv <username>@dumbo.es.its.nyu.edu:/home/<username>/project
scp ./hfi_data.csv <username>@dumbo.es.its.nyu.edu:/home/<username>/project

# Transfer to HDFS
hdfs dfs -put gtd_data.csv /user/<username>/project
hdfs dfs -put hfi_data.csv /user/<username>/project

# Sharing folder
hdfs dfs -setfacl -R -m default:user:<username2>:rwx /user/<username>/project
hdfs dfs -setfacl -R -m user:<username2>:rwx /user/jk5804/project
hdfs dfs -setfacl -R -m default:user:<username2>:rwx /user/<username>/impala_gtd
hdfs dfs -setfacl -R -m user:<username2>:rwx /user/<username>/impala_gtd
