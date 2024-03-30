from pyspark.sql import *

if __name__ == '__main__':

    print("Hello Spark")
    spark=SparkSession.builder.appName('Hello Spark').master('local[2]').getOrCreate()

    data_list=[('Ravi',28),
                ('David',45),
                ('Abdul',37)]
    

    df=spark.createDataFrame(data_list).toDF('Name','Age')

    df.show()