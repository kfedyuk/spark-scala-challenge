import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}

object Main {

  def main(args: Array[String]): Unit = {

    //creates a SparkSession
    val spark = SparkSession
      .builder()
      .appName("Spark Scala Challenge")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //read googleplaystore_user_reviews.csv as dataframe
    val user_reviews = spark.read.options(
      Map("inferSchema"->"true","header"->"true")).csv("src/main/resources/googleplaystore_user_reviews.csv")
    //read googleplaystore.csv as dataframe
    val apps = spark.read.options(
      Map("inferSchema"->"true","header"->"true")).csv("src/main/resources/googleplaystore.csv")


    val df_1 = challenge_part1(user_reviews)
    challenge_part2(spark, apps)
    val df_3 = challenge_part3(apps)
    val new_df_3 = challenge_part4(df_1,df_3)
    challenge_part5(new_df_3)


    spark.stop()
  }

  private def challenge_part1(user_reviews: DataFrame) : DataFrame = {
    //create new dataframe whith average of the column Sentiment_Polarity and grouped by App name
    user_reviews.groupBy("App").agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity")).na.fill(0)
  }

  private  def challenge_part2 (spark: SparkSession, apps: DataFrame): Unit = {
    // This import is needed to use the $-notation
    import spark.implicits._

    //Filters apps that have rating greater or equal to 4.0 (added extra condition fo filter out NaN values)
    //and orders them by descending order
    val filtered_apps = apps.filter($"Rating" >= 4.0 && !$"Rating".isNaN).sort($"Rating".desc)

    //save filtered apps as csv (overwrites existing file), optionally repartition(1) can be added to save as only one cvs file
    filtered_apps.write.mode(SaveMode.Overwrite).options(Map("header" -> "true", "delimiter" -> "§")).csv("src/main/resources/best_apps.csv")
  }

  private def challenge_part3(apps: DataFrame) : DataFrame = {

    //auxiliary dataframe that groups apps by name and aggregates the categories
    val combined_categories = apps.groupBy(col("App")).agg(collect_set("Category") as "Categories")

    //auxiliary dataframe that groups apps by name and selects the highest rating
    val aux_max_rating = apps.groupBy("App").agg(max("Rating").alias("Rating"))

    //auxiliary dataframe that selects all the columns that have the same app name and the same rating
    val combined_rating = apps.join(aux_max_rating,  aux_max_rating("App") === apps("App") && aux_max_rating("Rating") === apps("Rating"), "leftsemi")

    //joins the data that has the same app name and highest rating with the aggregated categories, additionally modifies columns
    //into desired format
    val df = combined_rating.join(combined_categories, combined_rating("App") === combined_categories("App"), "inner")
        .select(combined_categories("App"), combined_categories("Categories"), combined_rating("Rating"), col("Reviews"),
          col("Size"), col("Installs"), col("Type"), col("Price"), col("Content Rating").alias("Content_Rating"),
          col("Genres"), col("Last Updated").alias("Last_Updated"), col("Current Ver").alias("Current_Version"),
          col("Android Ver").alias("Minimum_Android_Version"))
        .withColumn("Rating", col("Rating").cast(DoubleType))
        .withColumn("Reviews", col ("Reviews").cast(LongType))
        .withColumn("Price", col("Price").cast("Double")*0.9)
        .withColumn("Genres", split(col("Genres"), ";").cast("array<String>"))
        .withColumn("Last_Updated", date_format(to_date(col("Last_Updated"),"MMMM dd, yyyy"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("Size",  regexp_replace(combined_rating("Size"), "M", "").cast(DoubleType))


    df
  }

  private def challenge_part4(df_1 : DataFrame, df_3 : DataFrame): DataFrame = {

    //merged dataframes df_3 and df_1
    val combined_df = df_3.join(df_1, df_3("App") === df_1("App"), "inner")
        .select(df_3("App"), col("Categories"), col("Rating"), col("Reviews"), col("Size"),
          col("Installs"), col("Type"), col("Price"), col("Content_Rating"), col("Genres"),
          col("Last_Updated"), col("Current_Version"), col("Minimum_Android_Version"), col("Average_Sentiment_Polarity"))


    //store the merged dataframe as parquet file
    combined_df.write.mode(SaveMode.Overwrite).option("compression","gzip").parquet("src/main/resources/googleplaystore_cleaned")

    //since for challenge part 5 the Average_Sentiment_Polarity is required, the combined dataframe is returned
    combined_df

  }

  private def challenge_part5(df_3 : DataFrame): Unit = {

    //Convert collection of genres into rows
    val exploded_genres = df_3.select(col("*"), explode(col("Genres")).alias("Genre")).na.fill(0)

    //Create dataframe df_4 containing the number of applications, the average rating and the average sentiment polarity by genre
    val df_4 = exploded_genres.groupBy("Genre").agg(count("App").alias("Count"), avg("Rating").alias("Average_Rating"), avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    //store the merged dataframe as parquet file
    df_4.write.mode(SaveMode.Overwrite).option("compression","gzip").parquet("src/main/resources/googleplaystore_metrics")
  }

}
