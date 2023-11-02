"""
Spark scripts ini untuk melakukan ekstrak data dari file CSV yang sebelumnya telah didownload
melalui website kaggle.com, kemudian melakukan transformasi data agar sesuai yang dibutuhkan, 
serta melakukan load data ke database postgresql.
"""

# Mengimport library yang diperlukan
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import when, col, regexp_replace
from pyspark.sql.functions import monotonically_increasing_id


# Buat dulu sparksessionnya
ss = (SparkSession.builder
      .appName("ETLData")
      .getOrCreate()
     )

# Definisi untuk melakukan extract data dari file CSV
def extract_data():
    csv_file_path = '/data/spotify-2023.csv' 

    schema = StructType(
                [
                    StructField('track_name', StringType(), True),
                    StructField('artist_name', StringType(), True),
                    StructField('artist_count', IntegerType(), True),
                    StructField('released_year', IntegerType(), True),
                    StructField('released_month', IntegerType(), True),
                    StructField('released_day', IntegerType(), True),
                    StructField('in_spotify_playlists', IntegerType(), True),
                    StructField('in_spotify_charts', IntegerType(), True),
                    StructField('streams', LongType(), True),
                    StructField('in_apple_playlists', IntegerType(), True),
                    StructField('in_apple_charts', IntegerType(), True),
                    StructField('in_deezer_playlists', StringType(), True),
                    StructField('in_deezer_charts', IntegerType(), True),
                    StructField('in_shazam_charts', StringType(), True),
                    StructField('bpm', IntegerType(), True),
                    StructField('key', StringType(), True),
                    StructField('mode', StringType(), True),
                    StructField('danceability_percent', DoubleType(), True),
                    StructField('valence_percent', DoubleType(), True),
                    StructField('energy_percent', DoubleType(), True),
                    StructField('acousticness_percent', DoubleType(), True),
                    StructField('instrumentalness_percent', DoubleType(), True),
                    StructField('liveness_percent', DoubleType(), True),
                    StructField('speechiness_percent', DoubleType(), True)
                ]
            )

    df_spotify = ss.read.csv(csv_file_path, header=True, schema=schema)

    print("Data berhasil diekstrak dari CSV.")
    return df_spotify

# Definisi untuk melakukan transform data
def transform_data(df_spotify):
    df = df_spotify.withColumn('in_deezer_playlists',
        when(col('in_deezer_playlists').like('%,%'),
            regexp_replace(col('in_deezer_playlists'), ',', '')).otherwise(col('in_deezer_playlists')).cast("int")
    )

    df = df.withColumn('in_shazam_charts',
        when(col('in_shazam_charts').like('%,%'),
            regexp_replace(col('in_shazam_charts'), ',', '')).otherwise(col('in_shazam_charts')).cast("int")
    )

    df = df.withColumn('released_date',
        F.to_date(F.concat_ws('-', df['released_year'], df['released_month'], df['released_day']), 'yyyy-M-d')
    ).drop('released_year', 'released_month', 'released_day')

    df = df.withColumn("song_id", monotonically_increasing_id())

    df = df.select(
            "song_id", 
            "track_name", 
            "artist_name", 
            "artist_count", 
            "released_date",  
            "in_spotify_playlists", 
            "in_spotify_charts", 
            "streams", 
            "in_apple_playlists", 
            "in_apple_charts", 
            "in_deezer_playlists", 
            "in_deezer_charts", 
            "in_shazam_charts", 
            "bpm", 
            "key", 
            "mode", 
            "danceability_percent", 
            "valence_percent", 
            "energy_percent", 
            "acousticness_percent", 
            "instrumentalness_percent", 
            "liveness_percent", 
            "speechiness_percent"
        )

    df_songs = df.select(
                    "song_id",
                    "track_name", 
                    "artist_name",
                    "artist_count",
                    "released_date",
                    "streams"
                )
    df_characteristics = df.select(
                            'song_id',
                            'bpm',
                            'key',
                            'mode',
                            'danceability_percent',
                            'valence_percent',
                            'energy_percent',
                            'acousticness_percent',
                            'instrumentalness_percent',
                            'liveness_percent',
                            'speechiness_percent'
                        )
    df_playlists = df.select('song_id', 'in_spotify_playlists', 'in_apple_playlists', 'in_deezer_playlists')
    df_charts = df.select('song_id', 'in_spotify_charts', 'in_apple_charts', 'in_deezer_charts', 'in_shazam_charts')

    df_songs = df_songs.cache()
    df_characteristics = df_characteristics.cache()
    df_playlists = df_playlists.cache()
    df_charts = df_charts.cache()

    print("Data berhasil ditransform.")
    return df_songs, df_characteristics, df_playlists, df_charts

# Definisi untuk melakukan load data ke database postgresql
def load_data(df_songs, df_characteristics, df_playlists, df_charts):
    # Koneksikan ke postgresqlnya
    jdbc_url = "jdbc:postgresql://dibimbing-dataeng-postgres:5432/spotify"
    properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    table_songs = "songs"   
    table_characteristics = "characteristics"
    table_playlists = "playlists"
    table_charts = "charts"

    df_songs.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table=table_songs, mode="overwrite", properties=properties)

    df_characteristics.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table=table_characteristics, mode="overwrite", properties=properties)

    df_playlists.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table=table_playlists, mode="overwrite", properties=properties)

    df_charts.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table=table_charts, mode="overwrite", properties=properties)

    print("Data berhasil dimuat ke PostgreSQL.")

if __name__ == "__main__":
    df_spotify = extract_data()
    df_songs, df_characteristics, df_playlists, df_charts = transform_data(df_spotify)
    load_data(df_songs, df_characteristics, df_playlists, df_charts)
    ss.stop()
