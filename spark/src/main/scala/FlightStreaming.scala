import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object FlightStreaming {

  def main(args: Array[String]): Unit = {

    # ---------------------------------------------------------
    # 1. CREATE SPARK SESSION
    # ---------------------------------------------------------

    val spark = SparkSession.builder()
      .appName("FlightStream")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------------------------------------
    # 2. DEFINE THE JSON SCHEMA "created flightSchema here"
    # ---------------------------------------------------------

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

    # ---------------------------------------------------------
    # 3. READ FLIGHT DATA FROM KAFKA AS A STREAM
    # ---------------------------------------------------------

    val kafkaFlights = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka1:19092")
      .option("subscribe", "flight_states")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "1000")
      .load()

    # ---------------------------------------------------------
    # 4. CONVERT KAFKA VALUE FROM BINARY TO STRING
    # ---------------------------------------------------------

    val jsonFlights = kafkaFlights
      .selectExpr("CAST(value AS STRING) AS json")

    # ---------------------------------------------------------
    # 5. PARSE JSON INTO STRUCTURED COLUMNS 
    # ---------------------------------------------------------

    val parsedFlights = jsonFlights
      .select(from_json(col("json"), flightSchema).alias("flight"))
      .select("flight.*")

    # ---------------------------------------------------------
    # 6. CLEAN THE DATA
    # ---------------------------------------------------------

    val cleanedFlights = parsedFlights
      .filter(col("icao24").isNotNull)
      .filter(col("latitude").isNotNull)
      .filter(col("longitude").isNotNull)
      .filter(col("latitude").between(-90.0, 90.0))
      .filter(col("longitude").between(-180.0, 180.0))

    # ---------------------------------------------------------
    # 7. FEATURE ENGINEERING "coalesce()" & "flight_status"
    # ---------------------------------------------------------

    val transformedFlights = cleanedFlights
      .withColumn(
        "altitude",
        coalesce(col("baro_altitude"), col("geo_altitude"))
      )
      .withColumn(
        "flight_status",
        when(col("on_ground") === true, "GROUND")
          .otherwise("AIRBORNE")
      )

    # ---------------------------------------------------------
    # 8. DISPLAY THE STREAM
    # ---------------------------------------------------------

    val query = transformedFlights.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("numRows", "10")
      .option("checkpointLocation", "/tmp/flightstream/checkpoint")
      .start()

    query.awaitTermination()
  }
}