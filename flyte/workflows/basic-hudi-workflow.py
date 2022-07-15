from flytekit import Resources, task, workflow
from flytekitplugins.spark import Spark
import flytekit
import pandas as pd
@task(
    task_config=Spark(
        # this configuration is applied to the spark cluster
        spark_conf={
            "spark.driver.memory": "1000M",
            "spark.executor.memory": "1000M",
            "spark.executor.cores": "1",
            "spark.executor.instances": "2",
            "spark.driver.cores": "1",
            'spark.jars': '../../jars/hadoop-aws-3.2.0.jar,'
                          '../../jars/aws-java-sdk-bundle-1.11.375.jar,'
                          '../../jars/hudi-spark3.1.2-bundle_2.12-0.10.1.jar,'
                          '../../jars/spark-avro_2.12-3.1.2.jar'

        }
    ),
    limits=Resources(mem="2000M"),
    cache_version="1",
)
def hudiQery() -> pd.DataFrame:
    # if you run in local, you must set SPARK_LOCAL_IP=127.0.0.1. this connection to get jars dependences local file
    spark = flytekit.current_context().spark_session
    # hadoop 3.2.0
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZZQUVIBN74RMCUXK")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

    tripsSnapshotDF = spark. \
        read. \
        format("hudi"). \
        load("s3a://hudi-table/logs_mor")

    # tripsSnapshotDF.show()
    pandasDF = tripsSnapshotDF.toPandas()
    return pandasDF[['_hoodie_commit_time', '_hoodie_commit_seqno']]

@workflow
def my_spark() -> pd.DataFrame:
    return hudiQery()

if __name__ == "__main__":
    print(my_spark())
