CSE 512: Distributed Database Systems (Assignment 4)

Mapper (Map class in equijoin.java):

  Categorizing the data on the basis of join_column. i.e. key will the values of join_column and value will be the entire string.
  For example, sample input to mapper is:
  R,2,Don,Larson,Newark,555-3221 
  S,1,33000,10000,part1 
  S,2,18000,2000,part1 
  S,2,20000,1800,part1 
  R,3,Sal,Maglite,Nutley,555-6905 
  S,3,24000,5000,part1
  S,4,22000,7000,part1 
  R,4,Bob,Turley,Passaic,555-8908
  
  
  Output of Mapper:
  Key:         Value
  1  : S,1,33000,10000,part1
  2  : R,2,Don,Larson,Newark,555-3221
       S,2,18000,2000,part1
       S,2,20000,1800,part1
  3  : R,3,Sal,Maglite,Nutley,555-6905
       S,3,24000,5000,part1
  4  : S,4,22000,7000,part1
       R,4,Bob,Turley,Passaic,555-8908
  
  

Reducer (Reduce class in equijoin.java):
  From the output of Mapper, Reducer will first create two lists corresponding to the two tables.
  Then it combines the two lists. i.e. it will basically cross join the two lists. The output of the reducer will look like:
  
  R,2,Don,Larson,Newark,555-3221,S,2,18000,2000,part1 
  R,2,Don,Larson,Newark,555-3221,S,2,20000,1800,part1 
  R,3,Sal,Maglite,Nutley,555-6905,S,3,24000,5000,part1
  S,4,22000,7000,part1,R,4,Bob,Turley,Passaic,555-8908






Problem statement:


The required task is to write a map-reduce program that will perform equijoin. • The code should be in Java (use Java 1.8.x) using Hadoop Framework (use Hadoop 2.7.x). • The code would take two inputs, one would be the HDFS location of the file on which  the equijoin should be performed and other would be the HDFS location of the file, where the output should be stored. 
 
Format of the Input File: - Table1Name, Table1JoinColumn, Table1Other Column1, Table1OtherColumn2, …….. Table2Name, Table2JoinColumn, Table2Other Column1, Table2OtherColumn2, ……... 
 
Format of the Output File: - If Table1JoinColumn value is equal to Table2JoinColumn value, simply append both line side by side for Output. If Table1JoinColumn value does not match any value of Table2JoinColumn, simply remove them for the output file. You should not include two joins contains same row (No duplicate joins in output file). 
 
Note: - Table1JoinColumn and Table2JoinColumn would both be Integer or Real or Double or Float, basically Numeric. 
 
Example Input : - R, 2, Don, Larson, Newark, 555-3221 S, 1, 33000, 10000, part1 S, 2, 18000, 2000, part1 S, 2, 20000, 1800, part1 R, 3, Sal, Maglite, Nutley, 555-6905 S, 3, 24000, 5000, part1 S, 4, 22000, 7000, part1 R, 4, Bob, Turley, Passaic, 555-8908 Example Output: - R, 2, Don, Larson, Newark, 555-3221, S, 2, 18000, 2000, part1 R, 2, Don, Larson, Newark, 555-3221, S, 2, 20000, 1800, part1 R, 3, Sal, Maglite, Nutley, 555-6905, S, 3, 24000, 5000, part1 S, 4, 22000, 7000, part1, R, 4, Bob, Turley, Passaic, 555-8908 
 
Another correct answer is: R, 2, Don, Larson, Newark, 555-3221, S, 2, 18000, 2000, part1 R, 2, Don, Larson, Newark, 555-3221, S, 2, 20000, 1800, part1 R, 3, Sal, Maglite, Nutley, 555-6905, S, 3, 24000, 5000, part1 R, 4, Bob, Turley, Passaic, 555-8908, S, 4, 22000, 7000, part1 
 
So it means that whether R is before S is not required in the result. But you cannot have both  S, 4, 22000, 7000, part1, R, 4, Bob, Turley, Passaic, 555-8908 and  R, 4, Bob, Turley, Passaic, 555-8908, S, 4, 22000, 7000, part1 in the output.  

 You cannot assume that the table are R and S all the time. They can be other two tables. Number of tables in the input are exactly 2. 
