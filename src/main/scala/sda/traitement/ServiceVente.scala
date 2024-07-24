package sda.traitement
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.expressions._


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    // Question 1
   val spark = SparkSession
      .builder
      .appName("SDA")
      .config("spark.master","local")
      .getOrCreate()

    //import librairie
   import spark.implicits._
    // La fonction formatter utilisé pour diviser la colonne en deux
    def formatter()= {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

    /*Question n°2*/
    // la fonction calcul TTC pour calculer le TTC a parti des colonnes d'une datframe.

    def calculTTC () : DataFrame ={
      // verification de l'existance des colonnes
      if (!dataFrame.columns.contains("HTT") || !dataFrame.columns.contains("TVA")){
        throw new IllegalStateException("le DataFrame ne continet pas les colonnes 'HTT' et TVA. ")
      }
      dataFrame
        .withColumn("HTT", regexp_replace(col("HTT"), ",",".").cast("double"))
        .withColumn("TVA", regexp_replace(col("TVA"), ",", ".").cast("double"))
        .withColumn("TTC", round(col("HTT") + col("TVA") * col("HTT"),2))
        //suppression des colonnes
        .drop("HTT","TVA")
    }

    /*Question n°3*/
    def extractDateEndContratVille(): DataFrame = {
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, false)
        .add("Date_End_contrat", StringType, false)
      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)

      val dateFormat = "\\d{4}-\\d{2}-\\d{2}"
      dataFrame.select($"Id_Client", $"HTT_TVA", $"TTC", from_json($"MetaData",schema)as "metadata" )
        .select($"Id_Client", $"HTT_TVA",$"TTC", $"metadata.*")
        .select($"Id_Client", $"HTT_TVA",$"TTC", explode($"MetaTransaction"))
        .select($"Id_Client", $"HTT_TVA",$"TTC", $"col.Ville",regexp_extract($"col.Date_End_contrat", dateFormat, 0).as("Date_End_contrat"))
        .na.drop("any")
    }

    /*Question n°4*/
    def contratStatus(): DataFrame = {
      dataFrame.withColumn("Contrat_Status",when($"Date_End_contrat".lt(current_date()),"Expired")
        .otherwise("Actif"))
    }

  }

}