import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.clustering.KMeans

object FlightStreaming {

  def main(args: Array[String]): Unit = {

    // ---------------------------------------------------------
    // 1. CREATE SPARK SESSION
    // ---------------------------------------------------------

    val spark = SparkSession.builder()
      .appName("FlightStream")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ---------------------------------------------------------
    // 2. DEFINE THE JSON SCHEMA "created flightSchema here"
    // ---------------------------------------------------------

    val flightSchema = StructType(Seq(
      StructField("icao24", StringType, true),
      StructField("callsign", StringType, true),
      StructField("origin_country", StringType, true),

      StructField("time_position", LongType, true),
      StructField("last_contact", LongType, true),

      StructField("longitude", DoubleType, true),
      StructField("latitude", DoubleType, true),

      StructField("baro_altitude", DoubleType, true),
      StructField("on_ground", BooleanType, true),

      StructField("velocity", DoubleType, true),
      StructField("true_track", DoubleType, true),
      StructField("vertical_rate", DoubleType, true),

      StructField("sensors", StringType, true),
      StructField("geo_altitude", DoubleType, true),

      StructField("squawk", StringType, true),
      StructField("spi", BooleanType, true),
      StructField("position_source", IntegerType, true),
      StructField("category", StringType, true),

      StructField("snapshot_time", LongType, true),
      StructField("ingested_at", StringType, true)
    ))

    // ---------------------------------------------------------
    // 3. READ FLIGHT DATA FROM KAFKA AS A STREAM
    // ---------------------------------------------------------

    val kafkaFlights = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka1:19092")
      .option("subscribe", "flight_states")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "1000")
      .load()

    // ---------------------------------------------------------
    // 4. CONVERT KAFKA VALUE FROM BINARY TO STRING
    // ---------------------------------------------------------

    val jsonFlights = kafkaFlights
      .selectExpr("CAST(value AS STRING) AS json")

    // ---------------------------------------------------------
    // 5. PARSE JSON INTO STRUCTURED COLUMNS 
    // ---------------------------------------------------------

    val parsedFlights = jsonFlights
      .select(from_json(col("json"), flightSchema).alias("flight"))
      .select("flight.*")

    // ---------------------------------------------------------
    // 6. CLEAN THE DATA
    // ---------------------------------------------------------

    val cleanedFlights = parsedFlights
      .filter(col("icao24").isNotNull)
      .filter(col("latitude").isNotNull)
      .filter(col("longitude").isNotNull)
      .filter(col("latitude").between(-90.0, 90.0))
      .filter(col("longitude").between(-180.0, 180.0))
      .withColumn(
        "altitude",
        coalesce(col("baro_altitude"), col("geo_altitude"))
      )
      .filter(col("altitude").isNotNull)
      .filter(col("velocity").isNotNull)
      .filter(col("altitude") >= 0)
      .filter(col("velocity") >= 0)

    // ---------------------------------------------------------
    // 7. FEATURE ENGINEERING "coalesce()" & "flight_status"
    // ---------------------------------------------------------

    val transformedFlights = cleanedFlights
      .withColumn(
        "flight_status",
        when(col("on_ground") === true, "GROUND")
          .otherwise("AIRBORNE")
      )

    // =========================================================
    // ML TRAINING DATA
    // Read existing Kafka records as a batch
    // =========================================================

    val trainingKafka = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka1:19092")
      .option("subscribe", "flight_states")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    // =========================================================
    // Parse the training records
    // =========================================================

    val trainingFlights = trainingKafka
       .selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), flightSchema).alias("flight"))
       .select("flight.*")

    // =========================================================
    // Clean the training data
    // =========================================================

    val cleanTrainingFlights = trainingFlights
      .filter(col("icao24").isNotNull)
      .filter(col("latitude").isNotNull)
      .filter(col("longitude").isNotNull)
      .filter(col("latitude").between(-90.0, 90.0))
      .filter(col("longitude").between(-180.0, 180.0))
      .withColumn(
        "altitude",
        coalesce(col("baro_altitude"), col("geo_altitude"))
      )
      .filter(col("altitude").isNotNull)
      .filter(col("velocity").isNotNull)
      .filter(col("altitude") >= 0)
      .filter(col("velocity") >= 0)

    // ---------------------------------------------------------
    // Limit the training data
    // ---------------------------------------------------------

    val trainingData = cleanTrainingFlights
      .select(
        "icao24",
        "callsign",
        "origin_country",
        "latitude",
        "longitude",
        "altitude",
        "velocity"
      )
      .limit(20000) // Why 20,000? Because we don't need to train K-Means on everything and Kafka topic accumulates huge number of records while testing

    // ---------------------------------------------------------
    // Create the ML feature vector
    // ---------------------------------------------------------

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "latitude",
        "longitude",
        "altitude",
        "velocity"
      ))
      .setOutputCol("raw_features")

    val assembledTraining = assembler.transform(trainingData)

    // ---------------------------------------------------------
    // Scale the features
    // ---------------------------------------------------------

    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setWithMean(true)
      .setWithStd(true)

    val scalerModel = scaler.fit(assembledTraining)

    val scaledTraining = scalerModel.transform(assembledTraining)

    // ---------------------------------------------------------
    // Train K-Means
    // ---------------------------------------------------------

    val kmeans = new KMeans()
      .setK(4)
      .setSeed(42)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val kmeansModel = kmeans.fit(scaledTraining)

    // ---------------------------------------------------------
    // Check that the model actually works
    // ---------------------------------------------------------

    val trainingPredictions = kmeansModel
      .transform(scaledTraining)

    trainingPredictions
      .select(
        "icao24",
        "callsign",
        "latitude",
        "longitude",
        "altitude",
        "velocity",
        "prediction"
      )
      .show(10, truncate = false)

    // ---------------------------------------------------------
    // Apply the trained ML model to the live stream
    // ---------------------------------------------------------

    val liveAssembled = assembler.transform(transformedFlights)

    val liveScaled = scalerModel.transform(liveAssembled)

    val clusteredFlights = kmeansModel.transform(liveScaled)

    // ---------------------------------------------------------
    // 8. DISPLAY THE STREAM
    // ---------------------------------------------------------

    val query = clusteredFlights.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("numRows", "10")
      .option("checkpointLocation", "/tmp/flightstream/checkpoint")
      .start()

    query.awaitTermination()
  }
}