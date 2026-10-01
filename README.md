# Hive ORC Small File 문제 해결을 위한 Spark Partition Merge 초보자 가이드

## 1. 개요

Hive ORC 테이블에 데이터를 반복적으로 `INSERT`하면 하나의 파티션 안에 작은 ORC 파일이 계속 증가할 수 있습니다.

예를 들어 다음과 같은 Hive 테이블이 있다고 가정합니다.

```sql
CREATE TABLE dw.sales (
    id          BIGINT,
    customer_id STRING,
    amount      DECIMAL(18,2)
)
PARTITIONED BY (
    dt STRING
)
STORED AS ORC;
```

데이터를 반복해서 적재하면 다음처럼 하나의 파티션에 많은 파일이 생길 수 있습니다.

```text
/user/hive/warehouse/dw.db/sales/dt=2026-10-01/

000000_0.orc
000001_0.orc
000002_0.orc
000003_0.orc
...
000998_0.orc
000999_0.orc
```

각 파일의 크기가 몇 KB 또는 몇 MB밖에 되지 않는 경우 이를 **Small File Problem**이라고 합니다.

예:

```text
dt=2026-10-01

파일 수: 1,000개
전체 크기: 3GB

평균 파일 크기:
3MB
```

이러한 경우 Spark를 이용하여 해당 파티션만 읽은 뒤 ORC 파일을 몇 개의 큰 파일로 다시 만들 수 있습니다.

---

# 2. Small File이 왜 문제가 되는가?

HDFS와 Hive에서는 일반적으로 수많은 작은 파일보다 적당히 큰 파일 몇 개가 더 효율적입니다.

예를 들어 다음 두 상황을 비교해 보겠습니다.

## Small File 상태

```text
3GB 데이터

3MB × 1,000 files
```

## Merge 이후

```text
3GB 데이터

약 500MB × 6 files
```

두 경우 저장된 데이터 양은 같지만 두 번째 방식이 훨씬 효율적입니다.

Small File이 너무 많으면 다음과 같은 문제가 발생할 수 있습니다.

* HDFS NameNode 메타데이터 증가
* Hive 조회 성능 저하
* Spark 파일 스캔 오버헤드 증가
* Impala Scan Range 증가
* 파일 Open/Close 처리 증가
* Spark Task 수 증가
* HDFS NameNode Heap 사용량 증가

따라서 일정 주기로 Small File을 Merge하는 것이 좋습니다.

---

# 3. 기본 처리 방식

이번 가이드에서는 다음 방식으로 처리합니다.

```text
Hive ORC Partition
        │
        │ Spark Read
        ▼
+---------------------+
| Spark DataFrame     |
+---------------------+
        │
        │ coalesce()
        ▼
+---------------------+
| Temporary ORC      |
| Staging Directory  |
+---------------------+
        │
        │ Read Again
        ▼
+---------------------+
| INSERT OVERWRITE   |
| Target Partition   |
+---------------------+
        │
        ▼
Merged Hive Partition
```

처리 순서는 다음과 같습니다.

```text
1. Hive 파티션 읽기

2. Spark Partition 수 감소

3. 임시 Staging 디렉터리에 ORC 저장

4. Staging ORC 다시 읽기

5. INSERT OVERWRITE로 원본 파티션 교체

6. Row Count 검증

7. Staging 파일 삭제
```

---

# 4. 왜 바로 Overwrite하지 않는가?

초보자가 가장 주의해야 하는 부분입니다.

다음과 같이 하지 않는 것을 권장합니다.

```python
df = spark.sql("""
SELECT *
FROM dw.sales
WHERE dt = '2026-10-01'
""")

df.write \
    .mode("overwrite") \
    .insertInto("dw.sales")
```

Spark가 데이터를 읽고 있는 위치와 Overwrite 대상이 동일하기 때문입니다.

즉 다음 상황이 됩니다.

```text
읽기

dw.sales/dt=2026-10-01
        │
        ▼
      Spark
        │
        ▼
쓰기

dw.sales/dt=2026-10-01
```

Spark가 데이터를 아직 읽고 있는데 해당 디렉터리를 삭제하거나 덮어쓰면 문제가 발생할 수 있습니다.

따라서 다음처럼 **Staging 영역을 중간에 사용하는 방식**이 안전합니다.

```text
Hive Partition
      │
      ▼
Spark
      │
      ▼
Temporary ORC
      │
      ▼
Hive Partition Overwrite
```

---

# 5. 사전 조건

아래 예제는 다음 환경을 기준으로 합니다.

* Hive
* HDFS
* ORC
* Spark
* Hive Metastore 사용
* Partitioned Table
* 일반적인 Non-ACID Hive Table

Spark에서는 Hive 지원을 활성화해야 합니다.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HiveORCCompaction")
    .enableHiveSupport()
    .getOrCreate()
)
```

---

# 6. 예제 테이블

다음 테이블을 기준으로 설명합니다.

```sql
CREATE TABLE dw.sales (
    id          BIGINT,
    customer_id STRING,
    amount      DECIMAL(18,2)
)
PARTITIONED BY (
    dt STRING
)
STORED AS ORC;
```

데이터는 다음처럼 저장되어 있다고 가정합니다.

```text
dt=2026-09-29
dt=2026-09-30
dt=2026-10-01
```

우리는 이 중에서:

```text
dt=2026-10-01
```

파티션만 Merge할 것입니다.

---

# 7. 가장 간단한 Spark 처리 예제

먼저 이해하기 쉬운 최소 예제입니다.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HiveORCCompaction")
    .enableHiveSupport()
    .getOrCreate()
)

table_name = "dw.sales"

target_date = "2026-10-01"

staging_path = (
    "hdfs:///tmp/hive_compaction/"
    "dw_sales/dt=2026-10-01"
)

# -------------------------------------------------------
# 1. 대상 파티션 읽기
# -------------------------------------------------------

df = spark.sql(f"""
SELECT *
FROM {table_name}
WHERE dt = '{target_date}'
""")

# -----------------------------
```
