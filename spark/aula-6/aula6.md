

1. Abrir um arquivo CSV:
```
df=spark.read.csv("201707_Diarias.utf8.csv", header=True, sep="\t")
```

2. Criar uma UDF para transformar os números
```
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
to_value=lambda v: float(v.replace(",", "."))
udf_to_value = F.udf(to_value, FloatType())
```

3. Aplicar as transformações nas colunas
```
df2=df.withColumn("Valor", udf_to_value(df["Valor Pagamento"])) \
.withColumn("DtPg", F.to_date(df["Data Pagamento"], format="dd/MM/yyyy"))
```

4. Selecionar as colunas que interessam
```
df3=df2.select(df2["Nome Órgão Superior"].alias("Orgao"), 
    df2["Nome Função"].alias("Funcao"),
    df2["Nome Programa"].alias("Programa"), 
    df2["Nome Favorecido"].alias("Favorecido"), 
    df2["Valor"], df2["DtPg"])
```

5. Salvar em arquivo parquet
```
df3.write.parquet("governo/viagens/2017/08")
```
