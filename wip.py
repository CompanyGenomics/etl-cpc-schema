import polars as pl

print("Reading titles...")
titles = pl.read_parquet("data/processed/cpc_titles.parquet")
print("Titles shape:", titles.shape)

print("\nReading schema...")
schema = pl.read_parquet("data/output/cpc_schema_202505.parquet")
print("Schema shape:", schema.shape)

titles

(
    schema.select(pl.col("section", "class", "subclass"))
    .filter(pl.col("class").is_not_null())
    .filter(pl.col("subclass").is_not_null())
    .unique()
    .sort(pl.col("section", "class", "subclass"))
)

(
    schema.select(pl.col("section", "class"))
    .filter(pl.col("class").is_not_null())
    .unique()
    .sort(pl.col("section", "class"))
)

(
    schema.select(pl.col("section", "class", "subclass"))
    .drop_nulls()
    .unique()
    .sort(pl.col("section", "class", "subclass"))
)

(
    schema.select(pl.col("section", "level", "class", "subclass", "symbol"))
    .filter(pl.col("level")==0)
    .drop_nulls()
    .unique()
    .sort(pl.col("section", "class", "subclass", "symbol"))
)

schema.filter(pl.col("symbol" == 'A01'))
schema.filter(pl.col("symbol")==' Y10T70')