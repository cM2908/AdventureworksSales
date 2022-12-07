import org.apache.spark.sql.types.{StructType,StructField,_}
import org.apache.spark.sql.SparkSession

import scala.xml.XML
import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

//scala doesn't have any date-time library, so we have to use either java.util.Date or java.sql.Date or java.time.LocalDate

// java.util.Date  (Mutable & Not Thread-Safe) (Not recommanded to use)
// java.sql.Date (Immutable & Thread-Safe) (Recommanded to use) (Spark's default for DateType)
// java.time.LocalDate (Best among all) (Spark don't provide any Encoders)

//Spark currently (as of 3.3.0) does not support java.time.LocalDate & java.time.LocalDateTime, so we are wrapping it up with java.sql.Date & java.sql.Timestamp.

//Spark uses Encoders to serialize the data types & it did not provided the Encoders for java.time.LocalDate & java.time.LocalDateTime.

// So if we want to use them as it is, then we have to create custom Encoders for these two types.

// Direct conversion of string to java.sql.Date & java.sql.Timestamp is possible, but we are doing java.sql.Date(java.time.LocalDate(String)), because whenever spark starts supporting java.time.LocalDate we can just unwrap the java.sql.Date wrapper.

//save rdd in csv file format by first converting it to dataframe and then writing it.
//There is a problem when we try to write TimestampType to csv file, spark converts the format of TimestampType value to '2004-10-19T09:11:32.000+05:30' irrespective of whatever format we have, so to avoid that we have to specify option() method

object ExtractDemographics{

	case class CustomerWithDemographics(
		CustomerID:Int,
		AccountNumber:String,
		CustomerType:String,
		TerritoryName:String,
		CountryRegionCode:String,
		TerritoryGroup:String,
		TotalPurchaseYTD:Double, 
		DateFirstPurchase:Date, 
		BirthDate:Date, 
		MaritalStatus:String, 
		YearlyIncome:String, 
		Gender:String, 
		TotalChildren:Int, 
		NumberChildrenAtHome:Int, 
		Education:String, 
		Occupation:String, 
		HomeOwnerFlag:String, 
		NumberCarsOwned:Int, 
		CommuteDistance:String,
		ModifiedDate:Timestamp
	) extends java.io.Serializable	
	
	
	case class StoreWithDemographics(
		StoreID:Int,
		StoreName:String,
		TerritoryName:String,
		CountryRegionCode:String,
		TerritoryGroup:String,
		AnnualSales:Double, 
		AnnualRevenue:Double, 
		BankName:String, 
		BusinessType:String, 
		YearOpened:Int, 
		Specialty:String, 
		SquareFeet:Double, 
		Brands:String, 
		Internet:String, 
		NumberEmployees:Int, 
		ModifiedDate:Timestamp
	) extends java.io.Serializable
	
	
	def createCustomerWithDemographics(row:org.apache.spark.sql.Row):CustomerWithDemographics = {
		
		val CustomerID:Int = row.get(0).toString.toInt
		val AccountNumber:String = row.get(1).toString
		val CustomerType:String = row.get(2).toString
		val TerritoryName:String = row.get(4).toString
		val CountryRegionCode:String = row.get(5).toString
		val TerritoryGroup:String = row.get(6).toString
		val ModifiedDate:Timestamp = Timestamp.valueOf(LocalDateTime.parse(row.get(7).toString.substring(0,19),DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
		
		val xmlContent:String = row.get(3).toString
		
		val node = XML.loadString(xmlContent)
		
		val TotalPurchaseYTD:Double = (node \\ "IndividualSurvey" \\ "TotalPurchaseYTD").text.toDouble
		val DateFirstPurchase:Date = Date.valueOf(LocalDate.parse((node \\ "IndividualSurvey" \\ "DateFirstPurchase").text.substring(0,10)))
		val BirthDate:Date = Date.valueOf(LocalDate.parse((node \\ "IndividualSurvey" \\ "BirthDate").text.substring(0,10)))
		val MaritalStatus:String = (node \\ "IndividualSurvey" \\ "MaritalStatus").text
		val YearlyIncome:String = (node \\ "IndividualSurvey" \\ "YearlyIncome").text
		val Gender:String = (node \\ "IndividualSurvey" \\ "Gender").text
		val TotalChildren:Int = (node \\ "IndividualSurvey" \\ "TotalChildren").text.toInt
		val NumberChildrenAtHome:Int = (node \\ "IndividualSurvey" \\ "NumberChildrenAtHome").text.toInt
		val Education:String = (node \\ "IndividualSurvey" \\ "Education").text
		val Occupation:String = (node \\ "IndividualSurvey" \\ "Occupation").text
		val HomeOwnerFlag:String = (node \\ "IndividualSurvey" \\ "HomeOwnerFlag").text
		val NumberCarsOwned:Int = (node \\ "IndividualSurvey" \\ "NumberCarsOwned").text.toInt
		val CommuteDistance:String = (node \\ "IndividualSurvey" \\ "CommuteDistance").text
		
		CustomerWithDemographics(CustomerID,AccountNumber,CustomerType,TerritoryName,CountryRegionCode,TerritoryGroup,TotalPurchaseYTD, DateFirstPurchase, BirthDate, MaritalStatus, YearlyIncome, Gender, TotalChildren, NumberChildrenAtHome, Education, Occupation, HomeOwnerFlag, NumberCarsOwned, CommuteDistance, ModifiedDate)
	}



	def createStoreWithDemographics(row:org.apache.spark.sql.Row):StoreWithDemographics = {
		val StoreID:Int = row.get(0).toString.toInt
		val StoreName:String = row.get(1).toString
		val xmlDemographics:String = row.get(2).toString
		val TerritoryName:String = row.get(3).toString
		val CountryRegionCode:String = row.get(4).toString
		val TerritoryGroup:String = row.get(5).toString
		val ModifiedDate:Timestamp = Timestamp.valueOf(LocalDateTime.parse(row.get(6).toString.substring(0,19),DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
		
		val node = XML.loadString(xmlDemographics)
		
		val AnnualSales:Double = (node \\ "StoreSurvey" \\ "AnnualSales").text.toDouble
		val AnnualRevenue:Double = (node \\ "StoreSurvey" \\ "AnnualRevenue").text.toDouble
		val BankName:String = (node \\ "StoreSurvey" \\ "BankName").text
		val BusinessType:String = (node \\ "StoreSurvey" \\ "BusinessType").text
		val YearOpened:Int = (node \\ "StoreSurvey" \\ "YearOpened").text.toInt
		val Specialty:String = (node \\ "StoreSurvey" \\ "Specialty").text
		val SquareFeet:Double = (node \\ "StoreSurvey" \\ "SquareFeet").text.toDouble
		val Brands:String = (node \\ "StoreSurvey" \\ "Brands").text
		val Internet:String = (node \\ "StoreSurvey" \\ "Internet").text
		val NumberEmployees:Int = (node \\ "StoreSurvey" \\ "NumberEmployees").text.toInt
		
		StoreWithDemographics(StoreID,StoreName,TerritoryName,CountryRegionCode,TerritoryGroup,AnnualSales, AnnualRevenue, BankName, BusinessType, YearOpened, Specialty, SquareFeet, Brands, Internet, NumberEmployees,ModifiedDate)
	}


	
	def extractCustomerDemographics(spark:SparkSession,inputCustomerPath:String,outputCustomerPath:String):Unit = {
		import spark.implicits._
		val customerSchema = new StructType(
			Array(
				StructField("customer_id", IntegerType, true),
				StructField("account_number", StringType, true),
				StructField("customer_type", StringType, true),
				StructField("demographics", StringType, true),
				StructField("territory_name", StringType, true),
				StructField("country_region_code", StringType, true),
				StructField("territory_group", StringType, true),
				StructField("modified_date", TimestampType, true)
			))
		val customerDF = spark.read.schema(customerSchema).csv(inputCustomerPath)
		val customerRDD = customerDF.map(row => createCustomerWithDemographics(row))
		customerRDD.toDF.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(outputCustomerPath)
	}

	
	def extractStoreDemographics(spark:SparkSession,inputStorePath:String,outputStorePath:String):Unit = {
		import spark.implicits._
		val storeSchema = new StructType(
			Array(
				StructField("store_id", IntegerType, true),
				StructField("store_name", StringType, true),
				StructField("demographics", StringType, true),
				StructField("territory_name", StringType, true),
				StructField("country_region_code", StringType, true),
				StructField("territory_group", StringType, true),
				StructField("modified_date", TimestampType, true)
			))

		val storeDF = spark.read.schema(storeSchema).csv(inputStorePath)
		val storeRDD = storeDF.map(row => createStoreWithDemographics(row))
		storeRDD.toDF.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(outputStorePath)
	}

	
	def main(args:Array[String]):Unit={
		
		val inputCustomerPath:String = args(0)
		val outputCustomerPath:String = args(1)
	
		//val inputStorePath:String = args(2)
		//val outputStorePath:String = args(3)
		
		// Create Spark Session Object
		val spark = SparkSession.builder().appName("ExtractDemographics").getOrCreate()
	
		extractCustomerDemographics(spark,inputCustomerPath,outputCustomerPath)
		//extractStoreDemographics(spark,inputStorePath,outputStorePath)
		
		spark.stop
	}
}
