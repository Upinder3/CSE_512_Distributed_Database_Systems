PROJECT PHASE 1:
A major peer-to-peer taxi cab firm has hired your team to develop and run multiple spatial queries on their large database that contains geographic data as well as real-time location data of their customers. A spatial query is a special type of query supported by geodatabases and spatial databases. These queries differ from traditional SQL queries in that they allow for the use of points, lines, and polygons. These spatial queries also consider the relationship between these geometries. Since the database is large and mostly unstructured, your client wants you to use a popular Big Data software application, SparkSQL. The goal of this phase is to extract data from this database that will be used by your client for operational (day-to-day) and strategic level (long term) decisions.

In Project Phase 1, you need to write two User Defined Functions ST_Contains and ST_Within in SparkSQL and use them to do four spatial queries:

Range query: Use ST_Contains. Given a query rectangle R and a set of points P, find all the points within R.
Range join query: Use ST_Contains. Given a set of Rectangles R and a set of Points S, find all (Point, Rectangle) pairs such that the point is within the rectangle.
Distance query: Use ST_Within. Given a point location P and distance D in km, find all points that lie within a distance D from P
Distance join query: Use ST_Within. Given a set of Points S1 and a set of Points S2 and a distance D in km, find all (s1, s2) pairs such that s1 is within a distance D from s2 (i.e., s1 belongs to S1 and s2 belongs to S2).
A Scala SparkSQL code template is given. You must start from the template. A Scala SparkSQL code template is given. You must start from the template. The main code is in "SparkSQLExample.scala"

Sample Command:
./bin/spark-submit CSE512-Project-Phase1-Template-assembly-0.1.0.jar result/output rangequery src/resources/arealm10000.csv -93.63173,33.0183,-93.359203,33.219456 rangejoinquery src/resources/arealm10000.csv src/resources/zcta10000.csv distancequery src/resources/arealm10000.csv -88.331492,32.324142 1 distancejoinquery src/resources/arealm10000.csv src/resources/arealm10000.csv 0.1



PROJECT PHASE 2:
Two types of spatial data analysis. Hot zone analysis and hot cell/hotspot analysis.

Hot zone analysis
This task will needs to perform a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle will be obtained. The hotter rectangle means that it include more points. So this task is to calculate the hotness of all the rectangles.

Hot cell analysis
Description
This task will focus on applying spatial statistics to spatio-temporal big data in order to identify statistically significant spatial hot spots using Apache Spark. The topic of this task is from ACM SIGSPATIAL GISCUP 2016.

The Problem Definition page is here: http://sigspatial2016.sigspatial.org/giscup2016/problem

The Submit Format page is here: http://sigspatial2016.sigspatial.org/giscup2016/submit

Sample command:
./bin/spark-submit ~/GitHub/CSE512-Project-Hotspot-Analysis-Template/target/scala-2.11/CSE512-Project-Hotspot-Analysis-Template-assembly-0.1.0.jar test/output hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow_tripdata_2009-01_point.csv



PROJECT PHASE 3:
The  goal  of  this  phase  is  to  experimentally analyze  the  run  time  performance  on  a  single node  spark  cluster  and  multi-node  spark  cluster using  various  performance  indicators  like execution  time,  CPU  utilization  and  memory utilization.  This  testing  was  done  using  the open-source  tool  Ganglia  to  understand  the behaviour  of  the  above-mentioned  metrics  for  a user-defined   script   for   geospatial   data.
