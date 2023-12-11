# !pip install pyspark
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("TDE3")
spark = SparkContext.getOrCreate(conf=conf)

lines = spark.textFile("transactions_amostra.csv")

header = lines.first()
lines = lines.filter(lambda line: line != header)
fields = lines.map(lambda line: line.split(";"))

# Quantidade de transações envolvendo o Brasil
transacoes_brasil = fields.filter(lambda linha: linha[0] == 'Brazil').count()
resultado_formato_str = [f"Quantidade de transações envolvendo o Brasil: {transacoes_brasil}"]
spark.parallelize(resultado_formato_str).saveAsTextFile("exercício1.txt")

# Número de transações por ano
transacoes_por_ano = fields.map(lambda linha: (linha[1], 1)).reduceByKey(lambda contadorX, contadorY: contadorX + contadorY)
transacoes_por_ano.take(5)
transacoes_por_ano.saveAsTextFile("exercício2.txt")

# Número de transações por tipo de fluxo e ano
transacoes_por_fluxo_e_ano = fields.map(lambda linha: ((linha[4], linha[1]), 1)).reduceByKey(lambda contadorX, contadorY: contadorX + contadorY)
transacoes_por_fluxo_e_ano.take(5)
transacoes_por_fluxo_e_ano.saveAsTextFile("exercício3.txt")

# Média dos valores das commodities por ano
valor_medio_por_ano_map1 = fields.map(lambda linha: (linha[1], float(linha[5])))
valor_medio_por_ano_reduce = valor_medio_por_ano_map1.combineByKey(
    lambda valor: (valor, 1),
    lambda acumulado, valor: (acumulado[0] + valor, acumulado[1] + 1),
    lambda acumulado1, acumulado2: (acumulado1[0] + acumulado2[0], acumulado1[1] + acumulado2[1])
)
valor_medio_por_ano_map2 = valor_medio_por_ano_reduce.map(lambda acumulado: (acumulado[0], acumulado[1][0] / acumulado[1][1]))
valor_medio_por_ano_map2.take(5)
valor_medio_por_ano_map2.saveAsTextFile("exercicio4.txt")

# Preço médio das commodities por tipo de unidade, ano e categoria no fluxo de exportação no Brasil
preco_medio_exportacao_brasil = fields.filter(lambda linha: linha[0] == 'Brazil' and linha[4] == 'Export')
preco_medio_exportacao_brasil_map1 = preco_medio_exportacao_brasil.map(lambda linha: ((linha[7], linha[1], linha[8]), (float(linha[5]), 1)))
preco_medio_exportacao_brasil_reduce = preco_medio_exportacao_brasil_map1.reduceByKey(lambda acumulado, valor: (acumulado[0] + valor[0], acumulado[1] + valor[1]))
preco_medio_exportacao_brasil_map2 = preco_medio_exportacao_brasil_reduce.map(lambda acumulado: (acumulado[0], acumulado[1][0] / acumulado[1][1]))
preco_medio_exportacao_brasil_map2.take(5)
preco_medio_exportacao_brasil_map2.saveAsTextFile("exercicio5.txt")

# Preço de transação máximo, mínimo e médio por tipo de unidade e ano
preco_transacao_por_tipo_ano = fields.map(lambda linha: ((linha[2], linha[1]), float(linha[5])))
preco_stats_por_tipo_ano = preco_transacao_por_tipo_ano.combineByKey(
    lambda valor: (valor, valor, valor, 1),
    lambda x, valor: (max(x[0], valor), min(x[1], valor), x[2] + valor, x[3] + 1),
    lambda x, y: (max(x[0], y[0]), min(x[1], y[1]), x[2] + y[2], x[3] + y[3])
)
preco_medio_por_tipo_ano = preco_stats_por_tipo_ano.mapValues(lambda x: (x[0], x[1], x[2] / x[3]))
preco_medio_por_tipo_ano.take(5)
preco_medio_por_tipo_ano.saveAsTextFile("exercicio6.1.txt")

# Commodity mais comercializada (somando as quantidades) em 2016, por tipo de fluxo
mais_comercializados_2016 = fields.filter(lambda linha: linha[1] == '2016' and linha[6] != '')
mais_comercializados_2016_map = mais_comercializados_2016.map(lambda linha: ((linha[4], linha[3]), float(linha[6])))
mais_comercializados_2016_reduce = mais_comercializados_2016_map.reduceByKey(lambda acumulado, valor: acumulado + valor)
mais_comercializados_2016_reduce.take(5)
mais_comercializados_2016_reduce.saveAsTextFile("exercicio7.txt")

spark.stop()