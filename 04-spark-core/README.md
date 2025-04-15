# 04 - Loading and Preparing Electric Consumption Data with Spark Core

**Author:** Adrián Romero Flores  
**Repository:** [link](https://github.com/adrrf/bigdata/tree/main/04-spark-core)

Disclaimer: This document has been automatically generated from a Jupyter notebook and may contain formatting artifacts or code output inconsistencies. For the most accurate representation, refer to the original notebook in the repository.

---

## Overview

1. [Introduction](#introduction)  
2. [Environment Setup](#environment-setup)  
3. [Data Loading](#data-loading)  
4. [Data Preparation](#data-preparation)  
5. [Results and Conclusions](#results-and-conclusions)

## Introduction


This project aims to load and prepare electric consumption data using Spark Core. The data is provided in CSV format and contains information about electric consumption in a specific region. The goal is to load the data into a Spark DataFrame, perform some transformations, and save the results in a format suitable for further analysis.

The document structure is as follows, in section [Environment Setup](#environment-setup) we will talk about the execution environment. In section [Data Loading](#data-loading), we will load the data into a Spark DataFrame and perform some initial transformations. In section [Data Preparation](#data-preparation), we will perform additional transformations to prepare the data for analysis. Finally, in section [Results and Conclusions](#results-and-conclusions), we will summarize the results and provide conclusions based on the analysis.

## Environment Setup

For the execution of this notebook, we will use the DataBricks platform, which provides a cloud-based environment for running Spark applications. The notebook is written in Python and uses the PySpark library to interact with Spark. Here is a screenshot of the environment and the cluster details:
![Databricks Environment](./assets/env.png)
![Databricks Cluster](./assets/cluster.png)

## Data Loading

First, we need to upload the CSV to the Databricks environment. We can do this by using the "Upload Data" button in the Data section of the workspace.
![Databricks upload button](./assets/upload.png)


Once the file is uploaded, we can use the following code to load the data into a Spark DataFrame. The CSV file contains electric consumption data, and we will use the `spark.read.csv` method to read the file. We will also specify the schema of the DataFrame using the `schema` parameter. The schema is defined using the `StructType` and `StructField` from the `pyspark.sql.types` module.


```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# define the struct of types
schema = StructType([
    StructField("IDENTIFICADOR", StringType(), True),
    StructField("ANOMES", StringType(), True),
    StructField("CNAE", StringType(), True),
    StructField("PRODUCTO", StringType(), True),
    StructField("MERCADO", StringType(), True),
    StructField("ACTIVA_H1", DoubleType(), True),
    StructField("ACTIVA_H2", DoubleType(), True),
    StructField("ACTIVA_H3", DoubleType(), True),
    StructField("ACTIVA_H4", DoubleType(), True),
    StructField("ACTIVA_H5", DoubleType(), True),
    StructField("ACTIVA_H6", DoubleType(), True),
    StructField("ACTIVA_H7", DoubleType(), True),
    StructField("ACTIVA_H8", DoubleType(), True),
    StructField("ACTIVA_H9", DoubleType(), True),
    StructField("ACTIVA_H10", DoubleType(), True),
    StructField("ACTIVA_H11", DoubleType(), True),
    StructField("ACTIVA_H12", DoubleType(), True),
    StructField("ACTIVA_H13", DoubleType(), True),
    StructField("ACTIVA_H14", DoubleType(), True),
    StructField("ACTIVA_H15", DoubleType(), True),
    StructField("ACTIVA_H16", DoubleType(), True),
    StructField("ACTIVA_H17", DoubleType(), True),
    StructField("ACTIVA_H18", DoubleType(), True),
    StructField("ACTIVA_H19", DoubleType(), True),
    StructField("ACTIVA_H20", DoubleType(), True),
    StructField("ACTIVA_H21", DoubleType(), True),
    StructField("ACTIVA_H22", DoubleType(), True),
    StructField("ACTIVA_H23", DoubleType(), True),
    StructField("ACTIVA_H24", DoubleType(), True),
    StructField("ACTIVA_H25", DoubleType(), True),
    StructField("REACTIVA_H1", DoubleType(), True),
    StructField("REACTIVA_H2", DoubleType(), True),
    StructField("REACTIVA_H3", DoubleType(), True),
    StructField("REACTIVA_H4", DoubleType(), True),
    StructField("REACTIVA_H5", DoubleType(), True),
    StructField("REACTIVA_H6", DoubleType(), True),
    StructField("REACTIVA_H7", DoubleType(), True),
    StructField("REACTIVA_H8", DoubleType(), True),
    StructField("REACTIVA_H9", DoubleType(), True),
    StructField("REACTIVA_H10", DoubleType(), True),
    StructField("REACTIVA_H11", DoubleType(), True),
    StructField("REACTIVA_H12", DoubleType(), True),
    StructField("REACTIVA_H13", DoubleType(), True),
    StructField("REACTIVA_H14", DoubleType(), True),
    StructField("REACTIVA_H15", DoubleType(), True),
    StructField("REACTIVA_H16", DoubleType(), True),
    StructField("REACTIVA_H17", DoubleType(), True),
    StructField("REACTIVA_H18", DoubleType(), True),
    StructField("REACTIVA_H19", DoubleType(), True),
    StructField("REACTIVA_H20", DoubleType(), True),
    StructField("REACTIVA_H21", DoubleType(), True),
    StructField("REACTIVA_H22", DoubleType(), True),
    StructField("REACTIVA_H23", DoubleType(), True),
    StructField("REACTIVA_H24", DoubleType(), True),
    StructField("REACTIVA_H25", DoubleType(), True)
])
# read csv without header
df = spark.read.csv("/FileStore/tables/endesaAgregada.csv", header=False, schema=schema)
```

To understand the structure and content of the dataframe, we can use the following code that displays the schema, the first 5 rows, the number of rows and columns, and summary statistics for each column.


```python
# dataframe schema
for field in df.dtypes:
    print(f"column name: {field[0]}, type: {field[1]}")
# print 5 first rows
df.show(5)
# number of rows and columns
rows = df.count()
columns = len(df.columns)
print(f"number of rows: {rows}")
print(f"number of columns: {columns}")
# some stats -> the format is not so good for visualizing...
df.describe().show()
```

    column name: IDENTIFICADOR, type: string
    column name: ANOMES, type: string
    column name: CNAE, type: string
    column name: PRODUCTO, type: string
    column name: MERCADO, type: string
    column name: ACTIVA_H1, type: double
    column name: ACTIVA_H2, type: double
    column name: ACTIVA_H3, type: double
    column name: ACTIVA_H4, type: double
    column name: ACTIVA_H5, type: double
    column name: ACTIVA_H6, type: double
    column name: ACTIVA_H7, type: double
    column name: ACTIVA_H8, type: double
    column name: ACTIVA_H9, type: double
    column name: ACTIVA_H10, type: double
    column name: ACTIVA_H11, type: double
    column name: ACTIVA_H12, type: double
    column name: ACTIVA_H13, type: double
    column name: ACTIVA_H14, type: double
    column name: ACTIVA_H15, type: double
    column name: ACTIVA_H16, type: double
    column name: ACTIVA_H17, type: double
    column name: ACTIVA_H18, type: double
    column name: ACTIVA_H19, type: double
    column name: ACTIVA_H20, type: double
    column name: ACTIVA_H21, type: double
    column name: ACTIVA_H22, type: double
    column name: ACTIVA_H23, type: double
    column name: ACTIVA_H24, type: double
    column name: ACTIVA_H25, type: double
    column name: REACTIVA_H1, type: double
    column name: REACTIVA_H2, type: double
    column name: REACTIVA_H3, type: double
    column name: REACTIVA_H4, type: double
    column name: REACTIVA_H5, type: double
    column name: REACTIVA_H6, type: double
    column name: REACTIVA_H7, type: double
    column name: REACTIVA_H8, type: double
    column name: REACTIVA_H9, type: double
    column name: REACTIVA_H10, type: double
    column name: REACTIVA_H11, type: double
    column name: REACTIVA_H12, type: double
    column name: REACTIVA_H13, type: double
    column name: REACTIVA_H14, type: double
    column name: REACTIVA_H15, type: double
    column name: REACTIVA_H16, type: double
    column name: REACTIVA_H17, type: double
    column name: REACTIVA_H18, type: double
    column name: REACTIVA_H19, type: double
    column name: REACTIVA_H20, type: double
    column name: REACTIVA_H21, type: double
    column name: REACTIVA_H22, type: double
    column name: REACTIVA_H23, type: double
    column name: REACTIVA_H24, type: double
    column name: REACTIVA_H25, type: double
    +-------------+------+----+--------+-------+---------+---------+---------+---------+---------+---------+---------+---------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
    |IDENTIFICADOR|ANOMES|CNAE|PRODUCTO|MERCADO|ACTIVA_H1|ACTIVA_H2|ACTIVA_H3|ACTIVA_H4|ACTIVA_H5|ACTIVA_H6|ACTIVA_H7|ACTIVA_H8|ACTIVA_H9|ACTIVA_H10|ACTIVA_H11|ACTIVA_H12|ACTIVA_H13|ACTIVA_H14|ACTIVA_H15|ACTIVA_H16|ACTIVA_H17|ACTIVA_H18|ACTIVA_H19|ACTIVA_H20|ACTIVA_H21|ACTIVA_H22|ACTIVA_H23|ACTIVA_H24|ACTIVA_H25|REACTIVA_H1|REACTIVA_H2|REACTIVA_H3|REACTIVA_H4|REACTIVA_H5|REACTIVA_H6|REACTIVA_H7|REACTIVA_H8|REACTIVA_H9|REACTIVA_H10|REACTIVA_H11|REACTIVA_H12|REACTIVA_H13|REACTIVA_H14|REACTIVA_H15|REACTIVA_H16|REACTIVA_H17|REACTIVA_H18|REACTIVA_H19|REACTIVA_H20|REACTIVA_H21|REACTIVA_H22|REACTIVA_H23|REACTIVA_H24|REACTIVA_H25|
    +-------------+------+----+--------+-------+---------+---------+---------+---------+---------+---------+---------+---------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
    |        32116|201507|  T1|      P1|     M1|    512.0|    392.0|    365.0|    377.0|    349.0|    343.0|    378.0|    360.0|    413.0|     392.0|     585.0|     542.0|     940.0|     977.0|    1146.0|    2074.0|     765.0|     451.0|     474.0|     534.0|     488.0|     687.0|    1053.0|    1477.0|       0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|
    |        27062|201507|  T1|      P1|     M1|      0.0|      0.0|      0.0|      0.0|      0.0|      0.0|      0.0|      0.0|      0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|       0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|
    |        86254|201507|  T1|     P38|     M2|    148.0|    137.0|    121.0|    137.0|     98.0|    103.0|     93.0|    105.0|    127.0|     158.0|     118.0|     163.0|     149.0|     190.0|     201.0|     204.0|     181.0|     170.0|     149.0|     155.0|     174.0|     216.0|     211.0|     210.0|       0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|
    |        73313|201503|  T1|     P13|     M2|    318.0|    272.0|    149.0|    148.0|    136.0|    141.0|    137.0|    163.0|    332.0|     281.0|     274.0|     148.0|     207.0|     281.0|     593.0|     342.0|     382.0|     356.0|     392.0|     371.0|     482.0|     508.0|     468.0|     346.0|       0.0|       50.0|       52.0|       53.0|       58.0|       44.0|       57.0|       50.0|       55.0|       70.0|        64.0|        52.0|        57.0|        52.0|        54.0|        84.0|        59.0|        51.0|        61.0|        60.0|        68.0|        62.0|        62.0|        55.0|        44.0|         0.0|
    |        46321|201411|  T1|      P1|     M1|     50.0|     28.0|     54.0|     27.0|     59.0|     29.0|     48.0|     34.0|     53.0|      32.0|      46.0|      45.0|      37.0|      43.0|      41.0|      29.0|      56.0|      30.0|      38.0|      40.0|      51.0|      31.0|      43.0|      37.0|       0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|        0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|         0.0|
    +-------------+------+----+--------+-------+---------+---------+---------+---------+---------+---------+---------+---------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+
    only showing top 5 rows
    
    number of rows: 1173369
    number of columns: 55
    +-------+------------------+------------------+-------+--------+-------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+-----------------+-----------------+------------------+-----------------+------------------+------------------+-----------------+------------------+-----------------+-----------------+------------------+-----------------+------------------+----------+------------------+------------------+------------------+-----------------+-----------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+------------------+-----------------+-----------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+------------+
    |summary|     IDENTIFICADOR|            ANOMES|   CNAE|PRODUCTO|MERCADO|         ACTIVA_H1|         ACTIVA_H2|         ACTIVA_H3|         ACTIVA_H4|         ACTIVA_H5|         ACTIVA_H6|         ACTIVA_H7|         ACTIVA_H8|         ACTIVA_H9|        ACTIVA_H10|       ACTIVA_H11|       ACTIVA_H12|       ACTIVA_H13|        ACTIVA_H14|       ACTIVA_H15|        ACTIVA_H16|        ACTIVA_H17|       ACTIVA_H18|        ACTIVA_H19|       ACTIVA_H20|       ACTIVA_H21|        ACTIVA_H22|       ACTIVA_H23|        ACTIVA_H24|ACTIVA_H25|       REACTIVA_H1|       REACTIVA_H2|       REACTIVA_H3|      REACTIVA_H4|      REACTIVA_H5|       REACTIVA_H6|       REACTIVA_H7|       REACTIVA_H8|       REACTIVA_H9|      REACTIVA_H10|      REACTIVA_H11|     REACTIVA_H12|      REACTIVA_H13|     REACTIVA_H14|     REACTIVA_H15|      REACTIVA_H16|      REACTIVA_H17|      REACTIVA_H18|      REACTIVA_H19|      REACTIVA_H20|      REACTIVA_H21|      REACTIVA_H22|      REACTIVA_H23|     REACTIVA_H24|REACTIVA_H25|
    +-------+------------------+------------------+-------+--------+-------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+-----------------+-----------------+------------------+-----------------+------------------+------------------+-----------------+------------------+-----------------+-----------------+------------------+-----------------+------------------+----------+------------------+------------------+------------------+-----------------+-----------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+------------------+-----------------+-----------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+------------+
    |  count|           1173369|           1173369|1173369| 1173369|1173369|           1173369|           1173369|           1173369|           1173369|           1173369|           1173369|           1173369|           1173369|           1173369|           1173369|          1173369|          1173369|          1173369|           1173369|          1173369|           1173369|           1173369|          1173369|           1173369|          1173369|          1173369|           1173369|          1173369|           1173369|   1173369|           1173369|           1173369|           1173369|          1173369|          1173369|           1173369|           1173369|           1173369|           1173369|           1173369|           1173369|          1173369|           1173369|          1173369|          1173369|           1173369|           1173369|           1173369|           1173369|           1173369|           1173369|           1173369|           1173369|          1173369|     1173369|
    |   mean|50087.853757002275|201488.48303133968|   null|    null|   null|236.76462349013823|191.16584339623768|167.94789789060388|156.65954103099708|151.95112534931468|153.43278116261806|165.55332764032457|198.12966764930727|226.38656807875444| 248.4581632035617|271.8575631365751|274.7902961472478|280.4643594640731|300.98008086117835|304.5461235979474| 286.4628914689241|276.69810093840897|286.2722489685683|304.20356469277783|325.3780554113838|352.6983152784844|382.72049883710923|359.4732428588108|301.26651292134017|       0.0|14.729103547136493|13.537945863577443|12.983217129479304|12.58706809196425|12.34965556444733|12.310241279597467|12.799043182494168|14.609240145256948| 17.50052711465873|21.451689962833516| 26.27960854599022|27.45436431335752| 27.62477830929571|25.99234597130144| 20.4907463040186| 19.04391244357061|20.202605488980875|24.730686169482915|27.127235336880386|27.509387498732284|24.358867500334508|22.709055719044905|20.356628647935985|17.10363875302654|         0.0|
    | stddev|28912.129623636032| 36.15787489052472|   null|    null|   null| 531.4217359903192| 493.6730050998439| 471.4702153109263|456.93905677151207| 451.7586557833312|455.30170619940054|452.47381127888406| 433.9850789712919|458.82189119403944|506.36375613339317|553.2205418063961|557.1242083361511|556.6918881874782| 547.0947749878788|511.1852981556901|489.06631866728674|483.54974008985937| 511.506848545233| 551.7788925117317| 587.651823714219| 591.490738580867| 613.7645878238274|619.8855146905289| 585.5705314556863|       0.0|101.95102598709991|  94.3172201518307| 90.50387607422563| 86.5462580981549|83.94653985869007| 83.35675509693348| 87.56567958536725| 98.89391069954507|126.18014898221465| 154.3354007488439|188.72138022317398|197.5414990644512|199.35890140955087|183.9767297590829|143.9475260506522|135.13104763484338| 142.9139716710366|176.11615554965428|192.25176740373627|193.24910617128214|161.19111018571198|144.01221134321665|134.56523693457248| 117.584034273921|         0.0|
    |    min|                 1|            201309|     T1|      P1|     M1|              -6.5|              -5.5|             -33.0|              -4.5|              -4.0|              -4.0|              -4.5|              -6.0|              -7.5|              -8.5|             -9.5|            -10.0|            -39.0|            -558.0|           -603.0|            -610.0|            -540.0|           -512.5|            -517.5|           -537.5|           -618.0|            -706.0|           -812.5|              -8.0|       0.0|               0.0|               0.0|               0.0|              0.0|              0.0|               0.0|               0.0|               0.0|               0.0|               0.0|               0.0|              0.0|               0.0|              0.0|              0.0|               0.0|               0.0|               0.0|               0.0|               0.0|               0.0|               0.0|               0.0|              0.0|         0.0|
    |    max|             99999|            201510|     T2|     P98|     M2|           75096.0|           72823.0|           67658.0|           61104.0|           56545.0|           46896.0|           39842.0|           39688.5|           39775.0|           40783.0|          39720.0|          41758.0|          44389.0|           48894.0|          54928.0|           52698.0|           49393.0|          48923.0|           51168.0|          50562.0|          52741.0|           60699.0|          64890.0|           85047.0|       0.0|           11933.0|           11698.0|           11096.0|           8556.0|           5992.0|            6298.0|            7203.0|            9541.0|           10819.0|            9963.0|           12699.0|          12775.0|           12795.0|          12533.0|           9989.0|            9720.0|           10137.0|           10640.0|           10455.0|            9657.0|            9749.0|           11477.0|           11940.0|          11917.0|         0.0|
    +-------+------------------+------------------+-------+--------+-------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+-----------------+-----------------+------------------+-----------------+------------------+------------------+-----------------+------------------+-----------------+-----------------+------------------+-----------------+------------------+----------+------------------+------------------+------------------+-----------------+-----------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+------------------+-----------------+-----------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+------------+
    


As we can see in the output, the DataFrame contains negative values in the `ACTIVA` columns, which indicates that there are some errors in the data. We will need to handle these errors in the next section.

## Data Preparation

First of all, let's remove the colums that are not needed for the analysis. We can do this using the `drop` method of the DataFrame.


```python
# let's remove the ACTIVA_H25 column
df = df.drop("ACTIVA_H25")
# let's remove all REACTIVA_HX columns
reactiva_columns = [f"REACTIVA_H{i}" for i in range(1, 25)]

df = df.drop(*reactiva_columns)
print(f"columns after removal: {len(df.columns)} columns")
```

    columns after removal: 30 columns


Once we have removed the unnecessary columns, we can proceed to handle the null values. We can use the `isnan` and `col(x).isNull()` funtions to check for null values in the DataFrame. In this case there wasn't any null or nan values in the dataset, so we can proceed to the next step.


```python
from pyspark.sql.functions import col, isnan, when, count

# there is any null value?
null_summary = df.select([
    count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns
])

null_summary.show()
```

    +-------------+------+----+--------+-------+---------+---------+---------+---------+---------+---------+---------+---------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+------------+
    |IDENTIFICADOR|ANOMES|CNAE|PRODUCTO|MERCADO|ACTIVA_H1|ACTIVA_H2|ACTIVA_H3|ACTIVA_H4|ACTIVA_H5|ACTIVA_H6|ACTIVA_H7|ACTIVA_H8|ACTIVA_H9|ACTIVA_H10|ACTIVA_H11|ACTIVA_H12|ACTIVA_H13|ACTIVA_H14|ACTIVA_H15|ACTIVA_H16|ACTIVA_H17|ACTIVA_H18|ACTIVA_H19|ACTIVA_H20|ACTIVA_H21|ACTIVA_H22|ACTIVA_H23|ACTIVA_H24|REACTIVA_H25|
    +-------------+------+----+--------+-------+---------+---------+---------+---------+---------+---------+---------+---------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+------------+
    |            0|     0|   0|       0|      0|        0|        0|        0|        0|        0|        0|        0|        0|        0|         0|         0|         0|         0|         0|         0|         0|         0|         0|         0|         0|         0|         0|         0|         0|           0|
    +-------------+------+----+--------+-------+---------+---------+---------+---------+---------+---------+---------+---------+---------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+----------+------------+
    


Finally, we are going to remove the negative values in the `ACTIVA` columns. We can do this using the `filter` method of the DataFrame. We will filter out the rows where the `ACTIVA` columns are less than 0. This will ensure that we only keep the rows with valid values.


```python
# is there any negative value in activa columns?
df_negatives = df.filter(
    " OR ".join([f"{columna} < 0" for columna in activa_columns])
)
print(f"number of rows with negative values: {df_filtered.count()}")
df = df.subtract(df_negatives)

df.describe().show()
```

    number of rows with negative values: 4
    +-------+------------------+------------------+-------+--------+-------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+------------------+-----------------+-----------------+-----------------+-----------------+------------------+------------------+------------------+------------------+------------------+-----------------+-----------------+------------------+-----------------+-----------------+------------+
    |summary|     IDENTIFICADOR|            ANOMES|   CNAE|PRODUCTO|MERCADO|         ACTIVA_H1|         ACTIVA_H2|         ACTIVA_H3|         ACTIVA_H4|         ACTIVA_H5|         ACTIVA_H6|         ACTIVA_H7|         ACTIVA_H8|        ACTIVA_H9|        ACTIVA_H10|       ACTIVA_H11|       ACTIVA_H12|       ACTIVA_H13|       ACTIVA_H14|        ACTIVA_H15|        ACTIVA_H16|        ACTIVA_H17|        ACTIVA_H18|        ACTIVA_H19|       ACTIVA_H20|       ACTIVA_H21|        ACTIVA_H22|       ACTIVA_H23|       ACTIVA_H24|REACTIVA_H25|
    +-------+------------------+------------------+-------+--------+-------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+------------------+-----------------+-----------------+-----------------+-----------------+------------------+------------------+------------------+------------------+------------------+-----------------+-----------------+------------------+-----------------+-----------------+------------+
    |  count|           1173365|           1173365|1173365| 1173365|1173365|           1173365|           1173365|           1173365|           1173365|           1173365|           1173365|           1173365|           1173365|          1173365|           1173365|          1173365|          1173365|          1173365|          1173365|           1173365|           1173365|           1173365|           1173365|           1173365|          1173365|          1173365|           1173365|          1173365|          1173365|     1173365|
    |   mean|50087.835313819654| 201488.4831386653|   null|    null|   null|236.76357399445186|191.16473134958005|167.94691847805245|156.65863946853707|151.95038116869006|153.43221717027524|165.55296604210966|198.12955900337917|226.3866567521615|248.45781022955347|271.8573108112139|274.7898901876228|280.4642191474946|300.9810826128272|304.54764161194515|286.46435124620217|  276.699013094817|286.27319205873704|304.20446749306484| 325.379383652998|352.7000532656079|382.72243036054425|359.4744286730898|301.2657139935144|         0.0|
    | stddev|28912.143205991142|36.157790023692854|   null|    null|   null| 531.4213745115978|  493.672561395343|471.46986047092713| 456.9389329456938|451.75875890085473| 455.3020376084403| 452.4742961539081|433.98563706424494|458.8225466010457| 506.3643195235059|553.2211669485033|557.1246500662065|556.6922468672002|547.0948755626207| 511.1852612351353| 489.0662492375315|483.54971071164283|511.50694449292024|  551.779004675807|587.6521035479079|591.4907895396049| 613.7644860279443|619.8853169320593|585.5705455531287|         0.0|
    |    min|                 1|            201309|     T1|      P1|     M1|               0.0|               0.0|               0.0|               0.0|               0.0|               0.0|               0.0|               0.0|              0.0|               0.0|              0.0|              0.0|              0.0|              0.0|               0.0|               0.0|               0.0|               0.0|               0.0|              0.0|              0.0|               0.0|              0.0|              0.0|         0.0|
    |    max|             99999|            201510|     T2|     P98|     M2|           75096.0|           72823.0|           67658.0|           61104.0|           56545.0|           46896.0|           39842.0|           39688.5|          39775.0|           40783.0|          39720.0|          41758.0|          44389.0|          48894.0|           54928.0|           52698.0|           49393.0|           48923.0|           51168.0|          50562.0|          52741.0|           60699.0|          64890.0|          85047.0|         0.0|
    +-------+------------------+------------------+-------+--------+-------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+-----------------+------------------+-----------------+-----------------+-----------------+-----------------+------------------+------------------+------------------+------------------+------------------+-----------------+-----------------+------------------+-----------------+-----------------+------------+
    


Now, let's visualize some data, for example, we can view the aggregated consumption per hour grouped by cnae. We can use the `groupBy` method of the DataFrame to group the data by `CNAE`, and then use the `agg` method to calculate the sum of the `ACTIVA` columns. We will also use the `matplotlib` library to plot the data. 


```python
from pyspark.sql.functions import sum as _sum
import matplotlib.pyplot as plt

activa_columns = [f"ACTIVA_H{i}" for i in range(1, 25)]
agg_cnae = df.groupBy("CNAE").agg(*[_sum(col).alias(col) for col in activa_columns])

agg_cnae_pd = agg_cnae.toPandas()
agg_cnae_pd.set_index("CNAE", inplace=True)
agg_cnae_pd = agg_cnae_pd.transpose()
agg_cnae_pd.index = [f"H{i}" for i in range(1, 25)]

plt.figure(figsize=(10, 6))
for cnae_type in agg_cnae_pd.columns:
    plt.plot(agg_cnae_pd.index, agg_cnae_pd[cnae_type], marker='o', label=f'CNAE {cnae_type}')

plt.title("Evolución del Consumo Activo por Hora según Tipo CNAE")
plt.xlabel("Hora del Día")
plt.ylabel("Consumo Activo Total")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```


    
![Graph ACTIVA grouped by CNAE](./assets/output.png)
    


### Monitoring the Spark Jobs

We can monitor the Spark jobs using the Spark UI that Databricks provide. The Spark UI provides a web interface for monitoring and debugging Spark applications. It shows the status of the jobs, stages, and tasks, as well as the execution plan and performance metrics.
![Spark UI](./assets/sparkmonitoring.png)

## Results and Conclusions

Based on the results of the analysis, we can conclude that the electric consumption data contains some errors, such as negative values in the `ACTIVA` columns. We have successfully loaded and prepared the data for further analysis by removing unnecessary columns, handling null values, and filtering out invalid values. The data is now ready for further analysis and visualization. 

The visualization of aggregated hourly consumption segmented by CNAE, reveals discernible trends and consumption patterns. In the case of CNAE, the profile `T1` demonstrates a consumption pattern typical of residential or human usage: minimal activity during nocturnal hours, with notable increases during the late afternoon and evening—corresponding with common periods of occupancy and usage. Conversely, the `T2` profile reflects a pattern indicative of industrial or commercial activity, characterized by a peak in the morning hours followed by a significant decline during midday, aligning with standard lunch breaks.
