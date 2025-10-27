from pyspark.sql import SparkSession

# Identificador 
ru4655363 = 4655363  # RU

# Inicializa sessão Spark 
spark = SparkSession.builder \
    .appName("Trabalho Big Data") \
    .getOrCreate()

print(f"RU: {ru4655363}")

# Leitura do dataset
print("Carregando dataset...")
df = spark.read.csv(
    "/data/imdb-reviews-pt-br.csv",
    header=True,
    quote="\"",
    escape="\"",
    encoding="UTF-8"
)

df.show(5)

# QUESTÃO 1: Soma dos IDs negativos
def map1(x):
    return (x[3], int(x[0]))

def reduceByKey1(x, y):
    return x + y

resultado1 = df.rdd.map(map1).reduceByKey(reduceByKey1).collect()
print("\nQuestão 1 - Soma dos IDs:")
print(resultado1)

# QUESTÃO 2: Diferença de contagem de palavras
def map2(x):
    return (x[3], (len(x[1].split()), len(x[2].split())))

def reduceByKey2(x, y):
    return (x[0] + y[0], x[1] + y[1])

resultado2 = df.rdd.map(map2).reduceByKey(reduceByKey2).collect()
print("\nQuestão 2 - Contagem de palavras:")
print(resultado2)

for sentimento, (en, pt) in resultado2:
    if sentimento == "neg":
        diferenca = pt - en
        print(f"\nDiferença (PT - EN) para textos negativos: {diferenca}")

spark.stop()
