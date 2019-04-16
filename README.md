# Using Flink and Redis to process and find the duplicated string
  
 # Flink
 I used flink to process the stream of string and convert them into upper case.
  
 # Redis:
  I used jedis connector to add the string into <k,v> database of redis and find the duplicate.
  You need to setup the redis connection.
   
 ## Redis connection:
  Change this according to your redis connection.
  String connectionStr = "localhost:6379";
