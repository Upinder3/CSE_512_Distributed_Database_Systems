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
