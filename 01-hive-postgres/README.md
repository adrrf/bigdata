# 01 - Distributed Analysis of Energy Data with Hive and PostgreSQL

---

## Overview

1. [Introduction](#introduction)
2. [Setup and Configuration](#setup-and-configuration)
3. [Data Ingestion and Storage](#data-ingestion-and-storage)
4. [Data Exploration and Analysis](#data-exploration-and-analysis)
5. [Conclusions](#conclusions)
6. [Screenshots](#screenshots)

## Introduction

TODO

## Setup and Configuration

TODO

## Data Ingestion and Storage

To ingest the datasets into the PostgreSQL database, we first need to upload the files to the Hive server.

```bash
# copy the datasets to the hive server
docker cp datasets/informations_households.csv hiveserver2:/opt/hive/data/warehouse/
docker cp datasets/daily_dataset.csv hiveserver2:/opt/hive/data/warehouse/
# verify the files are uploaded
docker exec -it hiveserver2 ls /opt/hive/data/warehouse/
```

On the Hive server, we can access the Hive CLI by running the following command:

```bash
docker exec -it hiveserver2 /bin/bash
hive
```

Now, we can create the tables and load the data into them.

```sql
CREATE TABLE clientes (
    LCLid STRING,
    stdorToU STRING,
    Acorn STRING,
    Acorn_grouped STRING,
    file STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE consumos (
    LCLid STRING,
    day DATE,
    energy_median FLOAT,
    energy_mean FLOAT,
    energy_max FLOAT,
    energy_count INT,
    energy_std FLOAT,
    energy_sum FLOAT,
    energy_min FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/opt/hive/data/warehouse/informations_households.csv' INTO TABLE clientes;
LOAD DATA INPATH '/opt/hive/data/warehouse/daily_dataset.csv' INTO TABLE consumos;
```

Once the data is loaded, we can start exploring and analyzing it.

## Data Exploration and Analysis

We will answer the following questions about the data.

1. List the first 10 records of each table: We will use the following SQL queries to display the 10 first records of each table:

```sql
SELECT * FROM clientes LIMIT 10;
SELECT * FROM consumos LIMIT 10;
```

Their results will be displayed below.

| clientes.lclid | clientes.stdortou | clientes.acorn | clientes.acorn_grouped | clientes.file |
| -------------- | ----------------- | -------------- | ---------------------- | ------------- |
| MAC005492      | ToU               | ACORN-         | ACORN-                 | block_0       |
| MAC001074      | ToU               | ACORN-         | ACORN-                 | block_0       |
| MAC000002      | Std               | ACORN-A        | Affluent               | block_0       |
| MAC003613      | Std               | ACORN-A        | Affluent               | block_0       |
| MAC003597      | Std               | ACORN-A        | Affluent               | block_0       |
| MAC003579      | Std               | ACORN-A        | Affluent               | block_0       |
| MAC003566      | Std               | ACORN-A        | Affluent               | block_0       |
| MAC003557      | Std               | ACORN-A        | Affluent               | block_0       |
| MAC003553      | Std               | ACORN-A        | Affluent               | block_0       |
| MAC003482      | Std               | ACORN-A        | Affluent               | block_0       |

| consumos.lclid | consumos.day | consumos.energy_median | consumos.energy_mean | consumos.energy_max | consumos.energy_count | consumos.energy_std | consumos.energy_sum | consumos.energy_min |
| -------------- | ------------ | ---------------------- | -------------------- | ------------------- | --------------------- | ------------------- | ------------------- | ------------------- |
| MAC000131      | 2011-12-15   | 0.48500001430511475    | 0.43204545974731445  | 0.8679999709129333  | 22                    | 0.23914580047130585 | 9.505000114440918   | 0.07199999690055847 |
| MAC000131      | 2011-12-16   | 0.14149999618530273    | 0.29616665840148926  | 1.1160000562667847  | 48                    | 0.281471312046051   | 14.2160005569458    | 0.03099999949336052 |
| MAC000131      | 2011-12-17   | 0.1014999970793724     | 0.18981249630451202  | 0.6850000023841858  | 48                    | 0.18840467929840088 | 9.111000061035156   | 0.06400000303983688 |
| MAC000131      | 2011-12-18   | 0.11400000005960464    | 0.21897916495800018  | 0.6759999990463257  | 48                    | 0.20291927456855774 | 10.51099967956543   | 0.06499999761581421 |
| MAC000131      | 2011-12-19   | 0.19099999964237213    | 0.32597917318344116  | 0.7879999876022339  | 48                    | 0.2592049539089203  | 15.647000312805176  | 0.06599999964237213 |
| MAC000131      | 2011-12-20   | 0.21799999475479126    | 0.35749998688697815  | 1.0770000219345093  | 48                    | 0.28759658336639404 | 17.15999984741211   | 0.06599999964237213 |
| MAC000131      | 2011-12-21   | 0.13050000369548798    | 0.23508332669734955  | 0.7049999833106995  | 48                    | 0.222069650888443   | 11.284000396728516  | 0.06599999964237213 |
| MAC000131      | 2011-12-22   | 0.08900000154972076    | 0.2213541716337204   | 1.093999981880188   | 48                    | 0.26723888516426086 | 10.625              | 0.06199999898672104 |
| MAC000131      | 2011-12-23   | 0.16050000488758087    | 0.29112499952316284  | 0.7490000128746033  | 48                    | 0.24907605350017548 | 13.973999977111816  | 0.06499999761581421 |
| MAC000131      | 2011-12-24   | 0.10700000077486038    | 0.16899999976158142  | 0.6129999756813049  | 47                    | 0.1506846696138382  | 7.942999839782715   | 0.06499999761581421 |

2. Count the number of households in each socioeconomic category (Acorn): We will use the `COUNT` and `GROUP BY` functions to count the number of households in each socioeconomic category. The query will look like this:

```sql
SELECT acorn, COUNT(*) AS num_houses
FROM clientes
GROUP BY acorn;
```

The results are:

| acorn   | num_houses |
| ------- | ---------- |
| ACORN-  | 2          |
| ACORN-A | 157        |
| ACORN-B | 25         |
| ACORN-C | 151        |
| ACORN-D | 292        |
| ACORN-E | 1567       |
| ACORN-F | 684        |
| ACORN-G | 205        |
| ACORN-H | 455        |
| ACORN-I | 51         |
| ACORN-J | 112        |
| ACORN-K | 165        |
| ACORN-L | 342        |
| ACORN-M | 113        |
| ACORN-N | 152        |
| ACORN-O | 103        |
| ACORN-P | 110        |
| ACORN-Q | 831        |
| ACORN-U | 49         |

3. Show the top 10 households with the most consumption records: As previously, we will use the `COUNT`and `GROUP BY` functions to count the number of consumption records for each household. Then we will use the `ORDER BY` function to sort the results in descending order and limit the output to the top 10 households.

```sql
SELECT lclid, COUNT(*) AS num_records
FROM consumos
GROUP BY lclid
ORDER BY num_records DESC
LIMIT 10;
```

The results are:

| lclid     | num_records |
| --------- | ----------- |
| MAC000149 | 1658        |
| MAC000155 | 1658        |
| MAC000157 | 1658        |
| MAC000152 | 1658        |
| MAC000151 | 1658        |
| MAC000147 | 1658        |
| MAC000148 | 1658        |
| MAC000153 | 1658        |
| MAC000156 | 1658        |
| MAC000145 | 1658        |

4. Total energy consumption per household: We will assume that the total energy consumption per household is the sum of all `energy_sum` for records for that household. For this, we will use the `SUM` function and `GROUP BY` clause.

```sql
SELECT lclid, SUM(energy_sum) AS total_energy_consumption
FROM consumos
GROUP BY lclid;
```

As this query shows all households and their total energy consumption, for better understanding and visualization we will show only the 5 households with the lowest energy consumption.

```sql
SELECT lclid, SUM(energy_sum) AS total_energy_consumption
FROM consumos
GROUP BY lclid
ORDER BY total_energy_consumption ASC
LIMIT 5;
```

The results are:

| lclid     | total_energy_consumption |
| --------- | ------------------------ |
| MAC004067 | 0.0                      |
| MAC002594 | 2.5659999437630177       |
| MAC005565 | 11.579999923706055       |
| MAC002110 | 13.811999753117561       |
| MAC000197 | 33.60199952684343        |

5. Mean average consumption by tariff type (Standard or Time-of-Use): For this, we will need to `JOIN` the `consumos` table with the `clientes` table on the `lclid` column. Then we will calculate the average consumption for each tariff type.

```sql
SELECT cl.stdortou, AVG(c.energy_sum) AS avg_consumption
FROM consumos c
JOIN clientes cl ON c.lclid = cl.lclid
GROUP BY cl.stdortou;
```

The results are:

| cl.stdortou | avg_consumption    |
| ----------- | ------------------ |
| Std         | 10.281588853956741 |
| ToU         | 9.49876177965262   |

6. Households with consumption greater than 5 kWh in at least one measurement: For this, we will use the `MAX` function to find the records where `energy_max` is greater than 5.

```sql
SELECT lclid, MAX(energy_max) AS max_consumption
FROM consumos
GROUP BY lclid
HAVING max_consumption > 5;
```

The results are that 172 households consumed more than 5 kWh in at least one measurement.

7. Mean average consumption by Acorn category:
8. Comparar el consumo de hogares con diferentes tipos de tarifa
9. Detectar hogares con consumo inconsistente (por debajo de 0.1 kWh durante más de 3 días seguidos)
10. Consumo total de energía por franja horaria (mañana, tarde, noche)
11. Final Boss (opcional): ¿Cuánto cambia el consumo medio entre días laborables y fines de semana?

## Conclusions

TODO

## Screenshots

TODO
