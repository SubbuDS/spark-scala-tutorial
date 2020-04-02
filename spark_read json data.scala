val sparkConf = new SparkConf().setAppName("MyApp").setMaster("local")
val sc = new SparkContext(sparkConf).set("spark.executor.cores", "4")
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

import org.apache.spark.sql.SparkSession

val spark = SparkSession.
builder().
appName("MyApp").
config("spark.executor.cores", "4").
getOrCreate()

val data = (1 to 5).toList
val rdd = sc.parallelize(data)
val cities = sc.parallelize(List("tokyo","new york","sydney","san francisco"))

val cities = sc.parallelize(List("tokyo","new york","paris","san francisco"))

val upcities = cities.map {x => x.toUpperCase}

upcities.collect.foreach(println)

val cities1 = sc.parallelize(List("tokyo","tokyo","paris","sydney"))
val cities2 = sc.parallelize(List("perth","tokyo","canberra","sydney"))

val cities = cities1.union(cities2)

cities.distinct.collect.foreach(println)

val pairRDD = sc.parallelize(List(("a", 1), ("b",2), ("c",3), ("a", 30), ("b",25), ("a",20)))

val sumRDD = pairRDD.reduceByKey((x,y) =>x+y)

sumRDD.collect()

val rdd = sc.parallelize(List(("a", "Larry"), ("b", "Curly"), ("c", "Moe")))

val keys = rdd.keys

keys.collect()

keys.collect.foreach(println)

val value = rdd.values

value.collect.foreach(println)


val data = Array((100,"Jim Hernandez"), (101,"Shane King"))

val employees = sc.parallelize(data)

val data2 = Array((100,"Glendale"), (101,"Burbank"))

val cities = sc.parallelize(data2)

val data3 = Array((100,"CA"), (101,"CA"), (102,"NY"))

val states = sc.parallelize(data3)

val record = employees.join(cities).join(states)

record.collect.foreach(println)

val record = employees.join(cities).rightOuterJoin(states)

record.collect.foreach(println)

// UNION


val  data = Array((103,"Mark Choi","Torrance","CA"), (104,"Janet Reyes","RollingHills","CA"))
val employees = sc.parallelize(data)

val  data1 = Array((105,"Lester Cruz","VanNuys","CA"), (106,"John White","Inglewood","CA"))
val employees2 = sc.parallelize(data1)

val rdd = sc.union((employees, employees2))

//Subtract

val data = Array((103,"Mark Choi","Torrance","CA"),  (104,"Janet Reyes","Rolling Hills","CA"),(105,"Lester Cruz","Van Nuys","CA"))

val rdd = sc.parallelize(data)


val data2 = Array((103,"Mark Choi","Torrance","CA"))
val rdd2 = sc.parallelize(data2)


val employees = rdd.subtract(rdd2)

employees.collect.foreach(println)


//collect
rdd.collect()



// count
rdd.count()

// take
rdd.take(2)

//foreach

rdd.collect.foreach(println)

// Reading json data

val sample_json = spark.read.json("/Users/iPhonec/Desktop/employee_data.json")

sample_json.printSchema
sample_json.show()

sample_json.except(sample_json).show

sample_json.select("deptno").show()

sample_json.createOrReplaceTempView("jsontbl")

val jsondf = spark.sql("select * from jsontbl").show()

jsondf.show()

val empinfo = spark.sql("select deptno, empno ,ename, sal from jsontbl").show()

sample_json.select("deptno").distinct.show

val df = spark.read.json("/Users/iPhonec/Desktop/example.json")

df.show()

df.printSchema()

val df1 = spark.read.json("/Users/iPhonec/Desktop/multi_line.json")

df1.show()

val mdf = spark.read.option("multiline", "true").json("/Users/iPhonec/Desktop/multi_line.json")

mdf.show()

mdf.show(false)

spark.read.option("charset", "UTF-16BE")



val dfDates = nesteddf.select(explode(df("dates")))

import org.apache.spark.sql.types._  

import org.apache.spark.sql.functions._  

val jsonSchema = new StructType()
        .add("battery_level", LongType)
        .add("c02_level", LongType)
        .add("cca3",StringType)
        .add("cn", StringType)
        .add("device_id", LongType)
        .add("device_type", StringType)
        .add("signal", LongType)
        .add("ip", StringType)
        .add("temp", LongType)
        .add("timestamp", TimestampType)

case class DeviceData (id: Int, device: String)

val eventsDS = Seq (
 (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
 (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
 (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
 (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
(4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
(5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
(6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
(7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
(8 ,""" {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
(9,"""{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }"""),
(10,"""{"device_id": 10, "device_type": "sensor-igauge", "ip": "68.28.91.22", "cca3": "USA", "cn": "United States", "temp": 32, "signal": 26, "battery_level": 7, "c02_level": 886, "timestamp" :1475600518 }"""),
(11,"""{"device_id": 11, "device_type": "sensor-ipad", "ip": "59.144.114.250", "cca3": "IND", "cn": "India", "temp": 46, "signal": 25, "battery_level": 4, "c02_level": 863, "timestamp" :1475600520 }"""),
(12, """{"device_id": 12, "device_type": "sensor-igauge", "ip": "193.156.90.200", "cca3": "NOR", "cn": "Norway", "temp": 18, "signal": 26, "battery_level": 8, "c02_level": 1220, "timestamp" :1475600522 }"""),
(13, """{"device_id": 13, "device_type": "sensor-ipad", "ip": "67.185.72.1", "cca3": "USA", "cn": "United States", "temp": 34, "signal": 20, "battery_level": 8, "c02_level": 1504, "timestamp" :1475600524 }"""),
(14, """{"device_id": 14, "device_type": "sensor-inest", "ip": "68.85.85.106", "cca3": "USA", "cn": "United States", "temp": 39, "signal": 17, "battery_level": 8, "c02_level": 831, "timestamp" :1475600526 }"""),
(15, """{"device_id": 15, "device_type": "sensor-ipad", "ip": "161.188.212.254", "cca3": "USA", "cn": "United States", "temp": 27, "signal": 26, "battery_level": 5, "c02_level": 1378, "timestamp" :1475600528 }"""),
(16, """{"device_id": 16, "device_type": "sensor-igauge", "ip": "221.3.128.242", "cca3": "CHN", "cn": "China", "temp": 10, "signal": 24, "battery_level": 6, "c02_level": 1423, "timestamp" :1475600530 }"""),
(17, """{"device_id": 17, "device_type": "sensor-ipad", "ip": "64.124.180.215", "cca3": "USA", "cn": "United States", "temp": 38, "signal": 17, "battery_level": 9, "c02_level": 1304, "timestamp" :1475600532 }"""),
(18, """{"device_id": 18, "device_type": "sensor-igauge", "ip": "66.153.162.66", "cca3": "USA", "cn": "United States", "temp": 26, "signal": 10, "battery_level": 0, "c02_level": 902, "timestamp" :1475600534 }"""),
(19, """{"device_id": 19, "device_type": "sensor-ipad", "ip": "193.200.142.254", "cca3": "AUT", "cn": "Austria", "temp": 32, "signal": 27, "battery_level": 5, "c02_level": 1282, "timestamp" :1475600536 }""")).toDF("id", "device").as[DeviceData]

eventsDS.show()

eventsDS.printSchema

val eventsFromJSONDF = Seq (
 (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
 (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
 (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
 (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
(4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
(5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
(6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
(7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
(8 ,""" {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
(9,"""{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }""")).toDF("id", "json")

eventsFromJSONDF.show()

val jsDF = eventsFromJSONDF.select($"id", get_json_object($"json", "$.device_type").alias("device_type"),
                                          get_json_object($"json", "$.ip").alias("ip"),
                                         get_json_object($"json", "$.cca3").alias("cca3"))


jsDF.show()

val devicesDF = eventsDS.select(from_json($"device", jsonSchema) as "devices")
.select($"devices.*")
.filter($"devices.temp" > 10 and $"devices.signal" > 15).show()


val devicesUSDF = devicesDF.select($"*").where($"cca3" === "USA").orderBy($"signal".desc, $"temp".desc).show()

val stringJsonDF = eventsDS.select(to_json(struct($"*"))).toDF("devices").show()


val iotparq = devicesDF
  .write
  .mode("overwrite")
  .format("parquet")
  .save("/Users/iPhonec/Documents/iot_parquet")


//Nested Structures

val schema = new StructType()
  .add("dc_id", StringType)                               // data center where data was posted to Kafka cluster
  .add("source",                                          // info about the source of alarm
    MapType(                                              // define this as a Map(Key->value)
      StringType,
      new StructType()
      .add("description", StringType)
      .add("ip", StringType)
      .add("id", LongType)
      .add("temp", LongType)
      .add("c02_level", LongType)
      .add("geo", 
         new StructType()
          .add("lat", DoubleType)
          .add("long", DoubleType)
        )
      )
    )                

val dataDS = Seq("""
{
"dc_id": "dc-101",
"source": {
    "sensor-igauge": {
      "id": 10,
      "ip": "68.28.91.22",
      "description": "Sensor attached to the container ceilings",
      "temp":35,
      "c02_level": 1475,
      "geo": {"lat":38.00, "long":97.00}                        
    },
    "sensor-ipad": {
      "id": 13,
      "ip": "67.185.72.1",
      "description": "Sensor ipad attached to carbon cylinders",
      "temp": 34,
      "c02_level": 1370,
      "geo": {"lat":47.41, "long":-122.00}
    },
    "sensor-inest": {
      "id": 8,
      "ip": "208.109.163.218",
      "description": "Sensor attached to the factory ceilings",
      "temp": 40,
      "c02_level": 1346,
      "geo": {"lat":33.61, "long":-111.89}
    },
    "sensor-istick": {
      "id": 5,
      "ip": "204.116.105.67",
      "description": "Sensor embedded in exhaust pipes in the ceilings",
      "temp": 40,
      "c02_level": 1574,
      "geo": {"lat":35.93, "long":-85.46}
    }
  }
}""").toDS()

dataDS.count()

dataDS.show()

val df = spark                  // spark session 
.read                           // get DataFrameReader
.schema(schema)                 // use the defined schema above and read format as JSON
.json(dataDS.rdd)   

df.printSchema

df.show()

val explodedDF = df.select($"dc_id", explode($"source"))
//display(explodedDF)

explodedDF.show()

explodedDF.printSchema




