from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, col, row_number

class Transformations:


    def dedup(self,df:DataFrame,dedup_col:List,cdc:str):

        df = df.withColumn("dedupKey",concat(*dedup_col))
        df = df.withColumn("dedupCounts",row_number().over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
        df = df.filter(col("dedupCounts") == 1)
        df = df.drop("dedupKey","dedupCounts")

        return df