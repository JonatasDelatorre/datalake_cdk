from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, mean, count, to_timestamp, round, year, month, dayofmonth, lit
from pyspark.sql.types import NumericType

spark = SparkSession.builder.appName("AppleHealth").getOrCreate()

# today = datetime.today()
# year = today.year
# month = today.month
# day = today.day

today_year = '2025'
today_month = '01'
today_day = '30'

source_bucket = 's3://health-datalake-dev-cleaned-ACCOUNT_ID'
destination_bucket = 's3://health-datalake-dev-curated-ACCOUNT_ID'

source_path = f"{source_bucket}/HealthAutoExport/year={int(today_year)}/month={int(today_month)}/day={int(today_day)}/"

df_health = spark.read.option("header", "true").parquet(source_path)

# Calculando kj para kcal com multiplicacao de 0.239
df_health = df_health.withColumn(
    "calorias_consumidas", col("dietary_energy_kJ") * 0.239
).withColumn(
    "calorias_gastas", (col("resting_energy_kj") + col("active_energy_kj")) * 0.239
).withColumn(
    "calorias_repouso_kcal", col("resting_energy_kj") * 0.239
).withColumn(
    "calorias_ativas_kcal", col("active_energy_kj") * 0.239
).withColumn(
    "batimentos_media_bpm", (col("heart_rate_max_bpm") + col("heart_rate_min_bpm")) / 2
)

# Renomeando colunas para portugues e dimensoes corretas
df_health = df_health.select(
    col("date").alias("data"),
    col("calorias_consumidas"),
    col("calorias_gastas"),
    col("calorias_repouso_kcal"),
    col("calorias_ativas_kcal"),
    col("walking__running_distance_km").alias("distancia_km"),
    col("walking_speed_kmhr").alias("velocidade_caminhada_km_hr"),
    col("heart_rate_max_bpm").alias("batimentos_max_bpm"),
    col("heart_rate_min_bpm").alias("batimentos_min_bpm"),
    col("batimentos_media_bpm"),
    col("apple_stand_hour_hours").alias("tempo_em_pe_horas")
)

# Forcando todas as colunas numericas a ficarem com 2 casas decimais
numeric_cols = [field.name for field in df_health.schema.fields if isinstance(field.dataType, NumericType)]

df_health = df_health.select(
    *[round(col(c), 2).alias(c) if c in numeric_cols else col(c) for c in df_health.columns]
)

df_health = df_health.withColumn('year', year(df_health['data'])) \
        .withColumn('month', month(df_health['data'])) \
        .withColumn('day', dayofmonth(df_health['data']))

df_health.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet(f"{destination_bucket}/HealthData/")


source_path = f"{source_bucket}/Workouts/year={int(today_year)}/month={int(today_month)}/day={int(today_day)}/"

df_workout = spark.read.option("header", "true").parquet(source_path)

df_workout = df_workout.select(
    'workout_type',
    'start',
    'duration',
    'active_energy_kcal',
    'intensity_kcalhrkg',
    'max_heart_rate_bpm',
    'avg_heart_rate_bpm',
    'step_count',
    'distance_km'
)

# Renomeando colunas para portugues e dimensoes corretas
df_workout = df_workout.withColumnRenamed('workout_type', 'tipo_de_exercicio') \
    .withColumnRenamed('start', 'data') \
    .withColumnRenamed('duration', 'duracao') \
    .withColumnRenamed('active_energy_kcal', 'energia_ativa_kcal') \
    .withColumnRenamed('intensity_kcalhrkg', 'intensidade_kcal_hr_kg') \
    .withColumnRenamed('max_heart_rate_bpm', 'batimento_maximo_bpm') \
    .withColumnRenamed('avg_heart_rate_bpm', 'batimento_medio_bpm') \
    .withColumnRenamed('step_count', 'quantidade_de_passos') \
    .withColumnRenamed('distance_km', 'distancia_km')

df_workout = df_workout.withColumn("duracao", to_timestamp(col("duracao"), "HH:mm:ss"))

df_agrupado = df_workout.groupBy("tipo_de_exercicio").agg(
    sum_("duracao").alias("duracao_min"),
    sum_("energia_ativa_kcal").alias("energia_ativa_kcal"),
    mean("intensidade_kcal_hr_kg").alias("intensidade_kcal_hr_kg"),
    mean("batimento_maximo_bpm").alias("batimento_maximo_bpm"),
    mean("batimento_medio_bpm").alias("batimento_medio_bpm"),
    sum_("quantidade_de_passos").alias("quantidade_de_passos"),
    sum_("distancia_km").alias("distancia_km")
)

df_agrupado = df_agrupado.withColumn("duracao_min", round(col("duracao_min") / 60, 2))

# Calculando a quantidade de exercícios de cada tipo feito no dia
quantidade_exercicio = df_workout.groupBy("tipo_de_exercicio").agg(
    count("*").alias("quantidade_no_dia")
)

df_agrupado = df_agrupado.join(quantidade_exercicio, on="tipo_de_exercicio", how="inner")

# Forcando todas as colunas numericas a ficarem com 2 casas decimais
numeric_cols = [field.name for field in df_agrupado.schema.fields if isinstance(field.dataType, NumericType)]

df_agrupado = df_agrupado.select(
    *[round(col(c), 2).alias(c) if c in numeric_cols else col(c) for c in df_agrupado.columns]
)

df_agrupado = df_agrupado.withColumn('year', lit(int(today_year))) \
        .withColumn('month', lit(int(today_month))) \
        .withColumn('day', lit(int(today_day)))

df_agrupado.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet(f"{destination_bucket}/Workouts/")
