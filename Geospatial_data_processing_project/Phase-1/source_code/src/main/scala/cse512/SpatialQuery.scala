package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle,pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle,pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1,pointString2,distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
  
def ST_Contains(queryRectangle: String, pointString: String): Boolean = {

  //removing spaces

  val rectangle = queryRectangle.replace(" ","").split(",")
  val x1_rect = rectangle(0).toDouble
  val y1_rect = rectangle(1).toDouble
  val x2_rect = rectangle(2).toDouble
  val y2_rect = rectangle(3).toDouble
  
  val points = pointString.replace(" ","").split(",")
  val x = points(0).toDouble
  val y = points(1).toDouble

  var x_cords = List(x1_rect, x2_rect)
  var y_cords = List(y1_rect, y2_rect)
  
  var min_x: Double = x_cords.min
  var min_y: Double = y_cords.min
  var max_x: Double = x_cords.max
  var max_y: Double = y_cords.max
  
  
  if(min_x<=x && x<= max_x && min_y<= y && y<= max_y)
    return true
  else
    return false  
}
                  
def ST_Within(pointString1: String , pointString2: String , distance: Double): Boolean = {
  
  val points_1 = pointString1.replace(" ","").split(",")
  val x1 = points_1(0).toDouble
  val y1 = points_1(1).toDouble
  
  val points_2 = pointString2.replace(" ","").split(",")
  val x2 = points_2(0).toDouble
  val y2 = points_2(1).toDouble
  
  val euc_dist = scala.math.pow(scala.math.pow((x1 - x2),2) + scala.math.pow((y1 - y2), 2), 0.5)
  
  if (euc_dist < distance)
    return true
  else
    return false
  
}
    
}
