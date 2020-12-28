hdfs dfs -rm -r impala_gtd
hdfs dfs -mkdir impala_gtd
hdfs dfs -rm -r impala_hfi
hdfs dfs -mkdir impala_hfi

hdfs dfs -put gtd_cleaned.txt impala_gtd
hdfs dfs -put hfi_cleaned.txt impala_hfi
 # on screenshot it says hdfs dfs -put hfi_cleaned_2.txt impala_hfi


impala-shell
connect compute-2-1;
# use <netid>;
use jk5804;
# or use <kk3506>;

source gtd_analytics.sql;
source hfi_analytics.sql;
source combined_analytics.sql;
source combined_analytics2.sql;
source combined_analytics3.sql;
