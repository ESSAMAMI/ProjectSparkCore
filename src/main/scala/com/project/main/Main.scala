package com.project.main

import com.sun.javaws.jnl.JavaFXAppDesc
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main{

  def main (args: Array[String]):Unit = {
    //Instancier le spark session
    val spark = SparkSession.builder()
      .appName("job-1")
      .master("local[2]")
      .getOrCreate();

    println("======================== READ DATASETS ========================")
    val clients = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("D:/Cours/4_IABD/SPARK/PROJECT_SPARK_CORE/src/data/clients.csv");
    clients.show();
    val items = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("D:/Cours/4_IABD/SPARK/PROJECT_SPARK_CORE/src/data/items.csv");
    items.show();
    val transaction = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("D:/Cours/4_IABD/SPARK/PROJECT_SPARK_CORE/src/data/transaction.csv");
    transaction.show();

    println("======================== Quels sont leurs plus gros clients ? ========================")

    val foreignKeyTransItem = transaction
      .select("Client_id", "Item_id", "Number")
    val dfTransItem = foreignKeyTransItem
      .join(items, foreignKeyTransItem("Item_id") === items("id")).select("Client_id","Item_Id", "Number", "Item_Buyed_Price", "Item_Selling_Price")

    val top = dfTransItem
      .withColumn("profit", (dfTransItem.col("Item_Selling_Price") * dfTransItem
        .col("Item_Buyed_Price")) * dfTransItem
        .col("Number"))
        .select("Client_id", "profit")
      .groupBy("Client_id").sum()

    //top.show(5)

    val topClient = top.orderBy(col("sum(profit)").desc)

    val topClients = topClient.join(clients, topClient("Client_id") === clients("id"))
      .withColumn("profits", round(col("sum(profit)") * 100 / 5) * 5 / 100)
      .select("Name", "First_Name", "Profits")

    topClients.show(5)

    // ================ SAVE DATAFRAME AS CSV FILE ====================
    val toCsvClients = topClients.coalesce(1).write.option("header","true").option("encoding", "ISO-8859-1").option("delimiter", ";")
      .option("inferSchema", "true").csv("D:/Cours/4_IABD/SPARK/CSV/topClients.csv")

    println("======================== Quels sont les produits qu’ils vendent le plus ? ========================")

    val topItemsSales = transaction.groupBy(col("Item_Id")).count() // AGG BY Item_Id
    topItemsSales.show(5)

    // Merge 2 dataframes !
    val joinItemsTransaction = transaction.join(items, transaction("Item_Id").equalTo(items("id")));
    //joinItemsTransaction.show()
    /*val columnsWeNeed = joinItemsTransaction.select(col("Item_Id"), col("Number"), col("Item_Name"), col("Category"), col("Item_Buyed_Price"), col("Item_Selling_Price"))
    columnsWeNeed.show()*/

    val bestItems = joinItemsTransaction
      .select(col("Item_Name"), col("Category"), col("Number"))
      .groupBy("Item_Name", "Category").agg(sum(col("Number")).alias("Total"))
      .orderBy(col("Total").desc)

    bestItems.show(5)

    // ================ SAVE DATAFRAME AS CSV FILE ====================
    val toCsvItems = bestItems.coalesce(1).write.option("header","true").option("encoding", "ISO-8859-1").option("delimiter", ";")
      .option("inferSchema", "true").csv("D:/Cours/4_IABD/SPARK/CSV/bestItems.csv")

    println("======================== Qui rapportent le plus ? ========================")
    /*
    * Items => ['id', 'Item_Name', 'Category', 'Category_Id', 'Item_Buyed_Price', 'Item_Selling_Price']
    *Transactions => ['Transaction_Id', 'Client_id', 'Item_Id', 'Number', 'Date_Of_Transaction', 'Shop_Name', 'Shop_Id']
     */

    val attNedeed = joinItemsTransaction
      .select(col("Item_Name"), col("Number"), col("Item_Selling_Price"))

    val itemSum = attNedeed
      .groupBy(col("Item_Name"))
      .agg(sum("Number").alias("Total_Items"), sum("Item_Selling_Price").alias("Amount"))
      .orderBy(col("Amount").desc)

    itemSum.show(5)

    // ================ SAVE DATAFRAME AS CSV FILE ====================
    val toCsvItem = itemSum.coalesce(1).write.option("header","true").option("encoding", "ISO-8859-1").option("delimiter", ";")
      .option("inferSchema", "true").csv("D:/Cours/4_IABD/SPARK/CSV/bestSales.csv")


  }
}