from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.feature import Normalizer, VectorAssembler
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


telemetry_path = 's3a://your-bucket/PdM_telemetry.parquet'
failures_path = 's3a://your-bucket/PdM_failures.parquet'

result_path = 's3a://your-another-bucket/data/partitions/data.csv'


# create the Spark session
spark = SparkSession\
    .builder\
    .appName('PM Training')\
    .getOrCreate()


# read datasets
failures = spark.read.parquet(failures_path)
telemetry = spark.read.parquet(telemetry_path)


# create tables
telemetry.createOrReplaceTempView('telemetry')
failures.createOrReplaceTempView('failures')

# join telemetry and failures tables
spark.sql(
    """
    SELECT 
      telemetry.datetime, telemetry.machineID, telemetry.volt,
      telemetry.rotate, telemetry.pressure, telemetry.vibration, failures.failure
    FROM telemetry
    LEFT JOIN failures
      ON telemetry.machineID = failures.machineID
      AND telemetry.datetime = failures.datetime
    """
).createOrReplaceTempView('merged')

# label encode
failure_dict = {
    'None': 0, 'comp1': 1,
    'comp2': 2, 'comp3': 3, 'comp4': 4
}
spark.udf.register('label_failure', lambda x: failure_dict[str(x)], T.IntegerType())

spark.sql(
    """
    SELECT *, label_failure(failure) as failure_lbl
    FROM merged
    """
).createOrReplaceTempView('merged')


# backfill any failure for the previous 24 hours
spark.sql(
    """
    SELECT *, MAX(failure_lbl)
    OVER 
      (
        PARTITION BY machineID
        ORDER BY datetime DESC
        ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
      ) as label
    FROM merged
    ORDER BY datetime ASC
    """
).createOrReplaceTempView('merged')

# get the dataframe
df = spark.sql(
    """
    SELECT datetime, machineID, volt, rotate, pressure, vibration, label
    FROM merged
    """
)

# label 0 is majority, apply undersampling
major = df.filter(df.label == 0)
minor = df.filter(df.label.isin([1, 2, 3, 4]))

ratio = (major.count() * 4.)/minor.count()
sampled_major = major.sample(False, 1/ratio)

balanced = sampled_major.unionAll(minor)

# train test split
balanced = balanced.orderBy(F.rand())
# future operations will reference from here
balanced.persist()

train, test = balanced.randomSplit([0.8, 0.2], seed=42)

# define the steps
num_assembler = VectorAssembler(
    inputCols=['volt', 'rotate', 'pressure', 'vibration'],
    outputCol='numericFeatures')

scaler = Normalizer(inputCol='numericFeatures', outputCol='numericFeaturesScaled')

encoder = OneHotEncoder(inputCol='machineID', outputCol='MachineIDOneHot')

assembler = VectorAssembler(
    inputCols=['MachineIDOneHot', 'numericFeaturesScaled'],
    outputCol='features')

# mlp
layers = [104, 256, 5]
mlp = MultilayerPerceptronClassifier(maxIter=200, layers=layers, seed=42)

# create the pipeline
pipeline = Pipeline(stages=[num_assembler, scaler, encoder, assembler, mlp])

# train and get the model
model = pipeline.fit(train)

# test set inference
result = model.transform(test)

# eval
evaluator = MulticlassClassificationEvaluator()
f1 = evaluator.evaluate(result)
print(f'Test score: {f1}')

# save the results
# create year, month, and day columns for the partitioning
result = result\
    .withColumn('year', F.year('datetime'))\
    .withColumn('month', F.month('datetime'))\
    .withColumn('day', F.dayofmonth('datetime'))

# save the results
result.select(
    'datetime', 'year', 'month', 'day',
    'machineID', 'prediction')\
    .write\
    .mode('overwrite')\
    .option('header', True)\
    .partitionBy('year', 'month', 'day')\
    .csv(result_path)

spark.stop()
