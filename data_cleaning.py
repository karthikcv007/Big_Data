from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, trim, lower, when


spark = SparkSession.builder.appName("StudentDataCleaning").getOrCreate()


data = [
    ("Alice Smith", 85, "Math", "A"),
    ("alice smith", 85, "Math", "A"),  
    ("Bob Jones", None, "Physics", "C"),
    ("BOB JONES", 72, "PHYSICS", "C"),  
    (None, 90, "Chemistry", "A"),
    ("Charlie Brown", 95, " Math ", " A "),  
    ("  Diana Prince  ", 78, "Biology", "B"),
    ("Diana Prince", 78, "BIOLOGY", "B"),  
    ("Evan Tucker", 40, "Math", "F"),
    ("Fay Allen", 92, "Chemistry", "unknown"),  
    ("Gina Lopez", -1, "Physics", "D"),  
    ("Hank", 88, None, "B"),  
    ("Ivy Chen", 150, "Biology", "A"),  
    ("ivy chen", 150, "BIOLOGY", "A"), 
]

columns = ["StudentName", "Score", "Subject", "Grade"]

df = spark.createDataFrame(data, columns)
print("Original Data:")
df.show()

df_filled = df.fillna({
    "StudentName": "Unknown Student",
    "Score": 0,
    "Subject": "General",
    "Grade": "N/A"
})
print("After Filling Nulls:")
df_filled.show()

df_dropped = df.dropna(subset=["StudentName", "Subject"])  
print("After Dropping Critical Nulls:")
df_dropped.show()

df_cleaned = df_dropped \
    .withColumn("StudentName", trim(lower(col("StudentName")))) \
    .withColumn("Subject", trim(lower(col("Subject")))) \
    .withColumn("Grade", trim(col("Grade")))

print("After Text Cleaning (trim + lower):")
df_cleaned.show()

df_dedup = df_cleaned.dropDuplicates()
print("After Removing Duplicates:")
df_dedup.show()

avg_score = df_dedup.select(avg("Score")).first()[0]
print(f"Average Score: {avg_score:.2f}")

outliers = df_dedup.filter((col("Score") > 100) | (col("Score") < 0))
print("Outliers Detected (Invalid Scores):")
outliers.show()

df_final = df_dedup.withColumn(
    "Score",
    when((col("Score") > 100) | (col("Score") < 0), avg_score).otherwise(col("Score"))
)

print("Final Data (Outliers Fixed with Average):")
df_final.show()
