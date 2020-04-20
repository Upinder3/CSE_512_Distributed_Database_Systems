The required task is to build a simplified query processor that accesses data from the partitioned ratings table. 
 
Input Data: - Same as in Assignment 1 i.e. ratings.dat file. 
 
Required Task: - Below are the steps you need to follow to fulfill this assignment: 
 
    RangeQuery() – o  Implement a Python function RangeQuery that takes as input: (1) RatingMinValue (2) RatingMaxValue (3) openconnection (4) outputPath o  Please note that the RangeQuery would not use ratings table but it would use the range and round robin partitions of the ratings table. o  RangeQuery() then returns all tuples for which the rating value is larger than or equal to RatingMinValue and less than or equal to RatingMaxValue. o  The returned tuples should be stored in outputPath. Each line represents a tuple that has the following format such that PartitionName represents the full name of the partition i.e. RangeRatingsPart1 or RoundRobinRatingsPart4 etc. in which this tuple resides. 
 
Example: 
PartitionName, UserID, MovieID, Rating 
 
RangeRatingsPart0,1,377,0.5 RoundRobinRatingsPart1,1,377,0.5
 o Note: Please use ‘,’ (COMMA, no space character) as delimiter between PartitionName, UserID, MovieID and Rating. 
 
    PointQuery() – o  Implement a Python function PointQuery that takes as input: (1) RatingValue. (2) openconnection (3) outputPath o  Please note that the PointQuery would not use ratings table but it would use the range and round robin partitions of the ratings table. o PointQuery() then returns all tuples for which the rating value is equal to RatingValue. o  The returned tuples should be stored in outputPath. Each line represents a tuple that has the following format such that PartitionName represents the full name of the partition i.e. RangeRatingsPart1 or RoundRobinRatingsPart4 etc. in which this tuple resides. 
 
Example 
PartitionName, UserID, MovieID, Rating 
 
RangeRatingsPart3,23,459,3.5 RoundRobinRatingsPart4,31,221,0
 o Note: Please use ‘,’ (COMMA, no space character) as delimiter between PartitionName, UserID, MovieID and Rating.
 
[More Information in the pdf file]
