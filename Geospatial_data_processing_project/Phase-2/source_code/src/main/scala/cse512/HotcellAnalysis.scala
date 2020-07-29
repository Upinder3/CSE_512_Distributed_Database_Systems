package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    //Points in the cube
    pickupInfo.createOrReplaceTempView("pickupinfo")
    val points = spark.sql("select x, y, z from pickupinfo where x >= " + minX + " and y >= " + minY  + " and z >= " + minZ + " and x <= " + maxX + " and y <= " + maxY +  " and z <= " + maxZ + " order by z, y, x").persist()
    points.createOrReplaceTempView("points")
    points.show()

    //Points and count for each group
    val countOfPoints = spark.sql("select x, y, z, count(*) as pointValues from points group by z, y, x order by z, y, x").persist()
    countOfPoints.createOrReplaceTempView("countOfPoints")
    countOfPoints.show()

    val dfForXNeighbors = spark.sql("SELECT t1.x, t1.y, t1.z, COUNT(t2.pointValues) as nNeighbors, SUM(t2.pointValues) as sumPV FROM countOfPoints t1, countOfPoints t2 WHERE ((ABS(t1.x-t2.x) = 0 OR ABS(t1.x-t2.x) = 1) AND (ABS(t1.y-t2.y) = 0 OR ABS(t1.y-t2.y) = 1) AND (ABS(t1.z-t2.z) = 0 OR ABS(t1.z-t2.z) = 1)) GROUP BY t1.x, t1.y, t1.z")
    dfForXNeighbors.show()
    dfForXNeighbors.createOrReplaceTempView("dfForXNeighbors")

    val sumX = spark.sql("SELECT SUM(countOfPoints.pointValues) as sumX FROM countOfPoints").first().getLong(0).toDouble
    val sumXsq = spark.sql("SELECT SUM(countOfPoints.pointValues * countOfPoints.pointValues) as sumXsq FROM countOfPoints").first().getLong(0).toDouble

    val meanX = (sumX / numCells.toDouble).toDouble

    val standardDeviationX = math.sqrt(sumXsq / numCells.toDouble - meanX * meanX)

    spark.udf.register("zScore", (x: Int, y: Int, z: Int, sumPV: Double, nNeighbors: Int) => ((HotcellUtils.zScore(x, y, z, minX, maxX, minY, maxY, minZ, maxZ, numCells.toDouble, sumPV.toDouble, nNeighbors, meanX.toDouble, standardDeviationX.toDouble))))

    val finalDF = spark.sql("SELECT tbl.x, tbl.y, tbl.z FROM (SELECT x, y, z, zScore(dfForXNeighbors.x, dfForXNeighbors.y, dfForXNeighbors.z, dfForXNeighbors.sumPV, dfForXNeighbors.nNeighbors) as zs FROM dfForXNeighbors ORDER BY zs DESC) tbl")
    finalDF.show()

    return finalDF

	// return pickupInfo 
	}
}
