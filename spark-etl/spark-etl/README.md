this is sample file 
URL: https://github.com/LearningJournal/Spark-Programming-In-Python


######## Setup ####
cd /Users/rajsingh/work/PYSPARK
python3 -m venv de-venv
source ./de-venv/bin/activate
pip3 install pyspark==3.0.3

#### Pyspark Shell #####
pyspark 

#### Daily RUN #######
cd /Users/rajsingh/work/PYSPARK
source ./de-venv/bin/activate

## How to RUN IT #
python3 modules/HelloSpark.py /Users/rajsingh/work/PYSPARK/
python3 modules/SparkSchemaDemo.py 

### Pytest 
#TestCase1: Make sure that we are loading the data file correctly 
#TestCase2: Record Count by Country is computed correctly
pytest 
### to run test cases 
cd /Users/rajsingh/work/PYSPARK/pyspark-repo/tests
pytest RowDemo_test.py -v