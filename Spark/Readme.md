This file talks about installation step required to install Pyspark and run the script "Tweets_preprocessing.py"

## Installation 

Using Jupyter notebook: 

conda create ---name envname
Check java is installation: java -version
conda install python=3.6
conda install pyspark  OR download jar and use "tar xf spark-3.2.0-bin-hadoop3.2.tgz". Then add path to .bashrc file.
pip install findspark
pip install spark-nlp==3.3.0
pip install jupyter notebook
Restart terminal.

### Using Command Line and not conda environment
install spark through  jar and not using conda 

update the .bashrc file: 
export SPARK_HOME=/home/path_to_spark/spark
export JAVA_HOME=/usr/
export SCALA_HOME=/usr/csshare/bin/scala/
export PATH=$PATH:${SPARK_HOME}/bin
export PATH=$PATH:$SCALA_HOME
export PYSPARK_HOME=${SPARK_HOME}/bin/pyspark


### Variable to update
LOG_DIR: path where logfile should be created
destpath: where files processedd would be saved in both parquet and csv format
input_path: path to pick the data files from

Command to run: ./bin/spark-submit --driver-memory 2G --deploy-mode client /home/xyz/scripts/Tweets_preprocessing.py 4


For Word cloud generation, Word cloud evaluation notebook was run in google collab, so google drive was attached to the session for fetching data. 
The NLP pipeline used pre-trained pipelines for lemmatizing and part of speech tagging.
Downloading them with active Spark session killed the spark session abruptly in google collab.  
So it was manually downloaded and stored in google drive for NLP pipeline to use. 