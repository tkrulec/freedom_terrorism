# PBDAA-Project: Exploring relationship between terrorism and freedom
Jiwon Kim (jk5804) and Tina Krulec (kk3506)

hadoop directories:
/user/jk5804/project
/user/kk3506/project

README.md - Describe your directories and files, how to build your code, how to run your code, where to find results of a run

## Contents - Describe your directories and files
1. /ana_code - A zip file containing source code for the analytic(s), class files, jar, etc.
    * analytics.sh
    * combined_analytics.sql
    * combined_analytics2.sql
    * combined_analytics3.sql
    * gtd_analytics.sql
    * hfi_analytics.sql
    * notes.txt --> further information on the some of the analytics we have made.
2. /data_ingest - Code and commands for data ingestion
    * transfer_data.sh - contains commands for transferring data to HDFS directories
    * gtd_data.csv - see "Datasets" section below [we were unable to add it to the zip file as it is too big, you can find it in /user/jk5804/project/gtd_data.csv]
    * hfi_data.csv - see "Datasets" section below
3. /etl_code - ETL/cleaning code
    * clean.sh - contains commands for cleaning datasets
    * CleanMapper_gtd.py
    * CleanReducer_gtd.py
    * CleanMapper_hfi.py
    * CleanReducer_hfi.py
    * gtd_cleaned.txt
    * hfi_cleaned.txt
    * hfi_cleaned_condensed.txt
4. /profiling_code - Profiling code  
    * profiling.sh
    * CountRecMapper_gtd.py
    * CountRecMapper_hfi.py
    * CountRecMapper.py
    * CountRecReducer.py
  Results Comparison
    * GTD dataset has a discrepancy between the number of rows in original and cleaned dataset. This is expected since we dropped rows to match the conditions and scope of the two datasets. Since HFI data only offered data only years 2008 and 2017, GTD data was matched accordingly.
    * HFI dataset, cleaned and original both have the same number of rows.
5. /test_code - Optional) Test code and unused code
    * experiments.sh
    * gtd_experiments.sql
    * /hfi_experiments
6. /screenshots - Screen shots that show your analytic running
    * /screenshots/gtd_analytics - for gtd analytics
    * /screenshots/hfi_analytics - for hfi analytics
    * /screenshots/combined_analytics - for combined analytics

## Datasets
1. Assignment of Datasets
    * Global Terrorism Database (GTD/gtd) - Jiwon we were unable to add it to the zip file as it is too big, you can find it in /user/jk5804/project/gtd_data.csv]
    * Human Freedom Index (HFI/hfi) - Tina
2. Obtaining Datasets
    * Global Terrorism Database (GTD) 1970-2018: “globalterrorismdb_0919dist.xlsx”
        * https://gtd.terrorismdata.com/files/gtd-1970-2018/
    * Human Freedom Index 2019 (HFI): “human-freedom-index-2019
        * https://www.cato.org/human-freedom-index-new  
3. Optional) Edit format of Datasets
    * “Globalterrorismdb_0919dist.xlsx” → “Globalterrorismdb_0919dist.csv”
4. Rename the files
    * “Globalterrorismdb_0919dist.csv” → “gtd_data.csv”
    * “Human-freedom-index-2019.csv” → “hfi_data.csv”

## Scripts - How to build and run your code
1. /ana_code - A zip file containing source code for the analytic(s), class files, jar, etc.
    * To run the code, run: analytics.sh
2. /data_ingest - Code and commands for data ingestion
    * To run the code, run: transfer_data.sh
3. /etl_code - ETL/cleaning code
    * To run the code, run: clean.sh
4. /profiling_code - Profiling code
    * To run the code, run: profiling.sh

## Results - Results of the code
Refer to the screenshots for the proof of runs on /screenshots
* /screenshots
    * /screenshots/gtd_analytics - for gtd analytics
    * /screenshots/hfi_analytics - for hfi analytics
    * /screenshots/combined_analytics - for combined analytics
Refer to the output files
* /data_ingest
    * gtd_data.csv
    * hfi_data.csv
* /etl_code
    * gtd_cleaned.txt
    * hfi_cleaned.txt
    * hfi_cleaned_condensed.txt [same code for cleaning, just less columns copied]
* /ana_code
This was an exploratory project mean to find correlations between the two data sets. There were simply too many correlations that we decided to run to make one or many cohesive tables as outputs of the analytic. However, we did put the results of the individual correlations in the code files. Screenshots also contain the results of the correlations.
We will describe the insights from our data exploration in our presentation and the paper.
