from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("data cleaning").getOrCreate()
data = [
    ("john", 25, "new york", "john@example.com"),
    ("Alice", None, "Los Angeles", "alice@example.com"),
    ("BOB", 30, None, "bob@example.com"),
    (None, 28, "Chicago", "noemail@example.com"),
    ("john", 25, "new york", "john@example.com"),
    ("  Eve", 22, " boston ", "eve@example"),
    ("Mallory", 35, "Dallas", None),
    ("ALICE", 29, "Los Angeles", "alice@example.com")
]
colunms=["Name","Age","City","Email"]
df=spark.createDataFrame(data,colunms)
df.show()
df_nodupli = df.fillna({
    "Name": "Unknown",
    "Age": 0,
    "City": "Unknown",
    "Email": "noemail@unknown.com"
})
df_nodupli.show()
df_nonull=df.dropna()
