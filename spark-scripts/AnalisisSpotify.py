"""
Spark scripts ini untuk melakukan ekstrak atau read data dari database yang sebelumnya telah dilakukan
pembersihan data dan pengubahan format data yang sesuai, kemudian melakukan analisis yang dibutuhkan, 
serta melakukan penyimpanan hasil dari analisis ke database kembali yang nantinya akan dilakukan
visualisasi dengan Metabase.
"""

# Mengimport library yang diperlukan
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import corr, col, expr


# Buat Sparksessionnya
ss = (SparkSession.builder
      .appName("AnalisisData")
      .getOrCreate()
     )

# Koneksikan juga ke postgresqlnya 
jdbc_url = "jdbc:postgresql://dibimbing-dataeng-postgres:5432/spotify"
properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Definisi untuk membaca data dari postgresql
def read_data_postgres():
    table_songs = "songs"
    table_characteristics = "characteristics"
    table_playlists = "playlists"
    table_charts = "charts"

    df_songs = ss.read.jdbc(url=jdbc_url, table=table_songs, properties=properties)
    df_characteristics = ss.read.jdbc(url=jdbc_url, table=table_characteristics, properties=properties)
    df_playlists = ss.read.jdbc(url=jdbc_url, table=table_playlists, properties=properties)
    df_charts = ss.read.jdbc(url=jdbc_url, table=table_charts, properties=properties)

    df_songs = df_songs.cache()
    df_characteristics = df_characteristics.cache()
    df_playlists = df_playlists.cache()
    df_charts = df_charts.cache()

    print("Data berhasil dibaca dari postgresql.")
    return df_songs, df_characteristics, df_playlists, df_charts

# Definisi untuk melakukan analisis dari data yang telah di read
def analisis(df_songs, df_characteristics, df_playlists, df_charts):
    # 1.Mencari 10 top song dengan streams terbanyak di spotify
    df_top10_songs_streams = (df_songs
                             .select("track_name", "artist_name", "streams")
                             .orderBy("streams", ascending=False)
                             .limit(10)
                            )
    
    # 2.Mencari 10 top song dengan kemunculan di playlists spotify terbanyak
    df_top10_songs_playlists = (df_songs
                               .alias("s")
                               .join(df_playlists.alias("p"), col("p.song_id") == col("s.song_id"), "full")
                               .select("track_name", "artist_name", "in_spotify_playlists")
                               .orderBy("in_spotify_playlists", ascending=False)
                               .limit(10)
                              )

    # 3.Mencari 10 Top Song dengan kemunculan di charts spotify terbanyak
    df_top10_songs_charts = (df_songs
                            .alias("s")
                            .join(df_charts.alias("c"), col("c.song_id") == col("s.song_id"), "full")
                            .select("track_name", "artist_name", "in_spotify_charts")
                            .orderBy("in_spotify_charts", ascending=False)
                            .limit(10)
                           )
    
    # 4.Mengetahui top 10 artis dengan lagu terbanyak
    df_top10_artists_songs = (df_songs
                              .groupBy("artist_name")
                              .count()
                              .orderBy("count", ascending=False)
                              .limit(10))
    df_top10_artists_songs = df_top10_artists_songs.withColumnRenamed("count", "songs_total")

    # 5.Menghitung nilai rata-rata dari setiap atribut pada top 10 songs
    ## a.Membuat dataframe dari top 10 songs dengan streams terbanyak dan menambahkan kolom-kolom atribut
    df_top10_streams_atribut = (df_songs
                                .alias("s")
                                .join(df_characteristics.alias("c"), col("c.song_id") == col("s.song_id"), "full")
                                .select(
                                    "track_name",
                                    "streams",
                                    "danceability_percent",
                                    "valence_percent",
                                    "energy_percent",
                                    "acousticness_percent",
                                    "liveness_percent",
                                    "speechiness_percent"
                                    )
                                .orderBy("streams", ascending=False)
                                .limit(10)
                               )

    ## b.Menghitung nilai rata-rata dari setiap atribut pada top 10 songs
    avg_danceability_top10songs = df_top10_streams_atribut.select("danceability_percent").agg({"danceability_percent": "avg"})
    avg_valence_top10songs = df_top10_streams_atribut.select("valence_percent").agg({"valence_percent": "avg"})
    avg_energy_top10songs = df_top10_streams_atribut.select("energy_percent").agg({"energy_percent": "avg"})
    avg_acousticness_top10songs = df_top10_streams_atribut.select("acousticness_percent").agg({"acousticness_percent": "avg"})
    avg_liveness_top10songs = df_top10_streams_atribut.select("liveness_percent").agg({"liveness_percent": "avg"})
    avg_speechiness_top10songs = df_top10_streams_atribut.select("speechiness_percent").agg({"speechiness_percent": "avg"})

    ## c.Mengubah nama kolom dari hasil perhitungan rata-rata pada setiap atribut
    avg_danceability = avg_danceability_top10songs.withColumnRenamed("avg(danceability_percent)", "avg_danceability")
    avg_valence = avg_valence_top10songs.withColumnRenamed("avg(valence_percent)", "avg_valence")
    avg_energy = avg_energy_top10songs.withColumnRenamed("avg(energy_percent)", "avg_energy")
    avg_acousticness = avg_acousticness_top10songs.withColumnRenamed("avg(acousticness_percent)", "avg_acousticness")
    avg_liveness = avg_liveness_top10songs.withColumnRenamed("avg(liveness_percent)", "avg_liveness")
    avg_speechiness = avg_speechiness_top10songs.withColumnRenamed("avg(speechiness_percent)", "avg_speechiness")

    ## d.Menggabungkan semua nilai rata-rata ke dalam satu dataframe
    df_avg_atribut = (avg_danceability
                      .join(avg_valence)
                      .join(avg_energy)
                      .join(avg_acousticness)
                      .join(avg_liveness)
                      .join(avg_speechiness)
                     )

    df_avg_atribut = df_avg_atribut.select(
                        expr("stack(6, 'Danceability', avg_danceability, 'Valence', avg_valence, 'Energy', avg_energy, 'Acousticness', avg_acousticness, 'Liveness', avg_liveness, 'Speechiness', avg_speechiness) as (atribut, avg_value)")
                    )
                                 
    # 6.Mengetahui korelasi streams dengan jumlah kemunculan di playlist spotify
    correlation_streams_playlists = (df_songs
                                    .alias("s")
                                    .join(df_playlists.alias("p"), col("p.song_id") == col("s.song_id"), "full")
                                    .select(corr("streams", "in_spotify_playlists").alias("correlation_value"))
                                   )
    
    # 7.Mengetahui korelasi streams dengan jumlah kemunculan di chart spotify
    correlation_streams_charts = (df_songs
                                 .alias("s")
                                 .join(df_charts.alias("c"), col("c.song_id") == col("s.song_id"), "full")
                                 .select(corr("streams", "in_spotify_charts").alias("correlation_value"))
                                )
    
    print("Semua analisis telah berhasil dilakukan.")
    return df_top10_songs_streams, df_top10_songs_playlists, df_top10_songs_charts, df_top10_artists_songs, df_avg_atribut, correlation_streams_playlists, correlation_streams_charts

# Definisi untuk menyimpan hasil analisisnya ke postgresql
def load_result_to_postgres(df_top10_songs_streams, df_top10_songs_playlists, df_top10_songs_charts, df_top10_artists_songs, df_avg_atribut, correlation_streams_playlists, correlation_streams_charts):
    table_top10_songs_streams = "top10_songs_streams"
    table_top10_songs_playlists = "top10_songs_playlists"
    table_top10_songs_charts = "top10_songs_charts"
    table_top10_artists_songs = "top10_artists_songs"
    table_avg_atribut_top10songs = "avg_atribut_top10songs"
    table_correlation_streams_playlists = "correlation_streams_playlists"
    table_correlation_streams_charts = "correlation_streams_charts"

    df_top10_songs_streams.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url,
              table=table_top10_songs_streams,
              mode="overwrite",
              properties=properties
              )

    df_top10_songs_playlists.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url,
              table=table_top10_songs_playlists,
              mode="overwrite",
              properties=properties
              )

    df_top10_songs_charts.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url,
              table=table_top10_songs_charts,
              mode="overwrite",
              properties=properties
              )

    df_top10_artists_songs.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url,
              table=table_top10_artists_songs,
              mode="overwrite",
              properties=properties
              )
    
    df_avg_atribut.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url,
              table=table_avg_atribut_top10songs,
              mode="overwrite",
              properties=properties
              )

    correlation_streams_playlists.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url,
              table=table_correlation_streams_playlists,
              mode="overwrite",
              properties=properties
              )

    correlation_streams_charts.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url,
              table=table_correlation_streams_charts,
              mode="overwrite",
              properties=properties
              )
    
    print("Hasil analisis berhasil dimuat ke PostgreSQL.")

if __name__ == "__main__":
    df_songs, df_characteristics, df_playlists, df_charts = read_data_postgres()
    df_top10_songs_streams, df_top10_songs_playlists, df_top10_songs_charts, df_top10_artists_songs, df_avg_atribut, correlation_streams_playlists, correlation_streams_charts = analisis(df_songs, df_characteristics, df_playlists, df_charts)
    load_result_to_postgres(df_top10_songs_streams, df_top10_songs_playlists, df_top10_songs_charts, df_top10_artists_songs, df_avg_atribut, correlation_streams_playlists, correlation_streams_charts)
    ss.stop()
