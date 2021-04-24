# Assignment 4. Open data

## Set up

For this assignment I reused the container created in [previous blog](index.md).

For Parts A and B of the assignment I added _zpln_ files from the assignment's GitHub to Zeppelin UI and started working on the tasks in the notebooks imported.

# Part A

In this part we will work with data provided by Nijmegen's city council and our goal will be to mis and match this data together, exploring the information that is implicit in the way the city is organized in quarters.

We start with downloading information about street names and their quarters and public artworks using the following shell comands (provided):

```
%sh
mkdir -p /opt/hadoop/share/data
cd /opt/hadoop/share/data
pwd
echo BAG data...
[ ! -f BAG_ADRES.csv ] \
  && wget --quiet https://rubigdata.github.io/course/data/BAG_ADRES.csv \
  && echo Downloaded BAG data || echo BAG data already exists
echo Artworks data...
[ ! -f kunstopstraat-kunstwerk.csv ] \
  && wget --quiet -O kunstopstraat-kunstwerk.csv https://www.nijmegen.nl/kos/opendata/ \
  && echo Downloaded artworks data || echo Artworks file already exists
```
All the information is now saved in _data_ folder in _.csv_ files. We start working with BAG (_basisadministratie adressen en gebouwen_) data, reding it with header from the file we just saved ans saving and variable _bagdata_. We used the following command for extracting the information and we print schema used for saving this information and first two rows ([the result](output-ass4.md)).


```
%spark
val bagdata = spark.read.format("csv").option("header", true).load("file:///opt/hadoop/share/data/BAG_ADRES.csv").cache()
```

In Spark we can analyse the data. In the following code examples we select the fields we are mostly interested in. For the example the following fields were selected (full list of possible fields is in the first part  [here](output-ass4.md)): STRAAT, HUISNUMMERTOEVOEGING, X_COORD and Y_COORD. We select the most interesting columns using _.select()_ command, we show the results using _.show()_ command and we can get some statistics using _.desribe().show()_ commands. Below it is possible to see commands used and outputs we get: 
```
%spark
val addrDF = bagdata.select('STRAAT, 'HUISNUMMERTOEVOEGING, 'X_COORD, 'Y_COORD)
addrDF.show(5, false)

output:
+-------------------------+--------------------+----------------------+----------------------+
|STRAAT                   |HUISNUMMERTOEVOEGING|X_COORD               |Y_COORD               |
+-------------------------+--------------------+----------------------+----------------------+
|Dr. Claas Noorduijnstraat|A25                 |188670,929999999990000|428403,169999999980000|
|Dr. Claas Noorduijnstraat|A24                 |188679,600000000010000|428375,099999999980000|
|Dr. Claas Noorduijnstraat|A23                 |188685,730000000010000|428373,049999999990000|
|Dr. Claas Noorduijnstraat|A22                 |188692,359999999990000|428389,729999999980000|
|Dr. Claas Noorduijnstraat|A21                 |188699,679999999990000|428403,510000000010000|
+-------------------------+--------------------+----------------------+----------------------+

addrDF.describe().show()

output:
+-------+--------------------+--------------------+--------------------+--------------------+
|summary|              STRAAT|HUISNUMMERTOEVOEGING|             X_COORD|             Y_COORD|
+-------+--------------------+--------------------+--------------------+--------------------+
|  count|               96867|                3814|               96822|               96822|
|   mean|                null|  151.20035149384887|                null|                null|
| stddev|                null|   95.25263997607337|                null|                null|
|    min|'s-Gravensandestraat|                   *|180685,8800000000...|423047,7700000000...|
|    max|        �vilastraat|                   Z|190871,2500000000...|433955,6200000000...|
+-------+--------------------+--------------------+--------------------+--------------------+
```

We can also get the statistics of validated fields for example using _.filter($"X_COORD".isNull).count_ and getting the number of rows where X coordinate was NULL or use _.show()_ instead of count and get the list of rows where X was missing.

We will use the following commands to show data records with missing X coordinate and it was recognised, that most records have status "Niet authentiek" and some have "Naamgeving uitgegeven". 

```
%spark
// Cell left empty for you to identify the records with missing coordinates
val addrDF1 = bagdata.select('STRAAT, 'STATUS, 'HUISNUMMERTOEVOEGING, 'X_COORD, 'Y_COORD)
addrDF1.filter( $"X_COORD".isNull )show()
```

When analysing such data it is needed to not include fields that do not have the values, as they can spoil the results. There is still need to describe this data, but it is good to do it separately from the main analysis to avoid errors. 

## Dataset API

To work with Dataset API we create the class, which described the objects we receive.

```
%spark
case class Addr(street:String, quarter:String, x:Float, y:Float)
```

For working with FloatType, we need to import the type: _import org.apache.spark.sql.types.{FloatType};_. We want to cast X and Y coordinates to FloatType, however, our data uses Dutch format for the number, using comma instead of dot and the result is NULL, because cast fails. We create a separate casting method to be able to translate numbers to FloatType: 

```
%spark
// Use Java's NumberFormat to parse values by their locale 
import java.text.NumberFormat
import java.util.Locale
val nf = NumberFormat.getInstance(Locale.forLanguageTag("nl")); 
// Handle floats written as 0,05 instead of 0.05

def convToFloat(s: String): Option[Float] = {
  try {
    Some(nf.parse(s).floatValue)
  } catch {
    case e: Exception => None
  }
}
```

When we created the method, we can specify the function used for casting and now we are able to receive and print  data correctly. When we get the data we cast it to Addr objects and print first two of them.

```
%spark
val tfloat = udf((f: String) => convToFloat(f).getOrElse(0f))

// Apply the transformation to the x and y columns
val addrDF = bagdata.select($"STRAAT" as "street",
                            tfloat($"X_COORD").cast(FloatType) as "x",
                            tfloat($"Y_COORD").cast(FloatType) as "y",
                            $"WIJK_OMS" as "quarter").as[Addr].cache()
addrDF.show(2)
```

When we introduced our own cast function, then all X and Y missing coordinates were casted to value 0.  

## Using Data frame operators

When we work with data, we are interested in statistics. We use the following commands to get to know what quarters in Nijmegen are the largest. Firstly, we group them, then order them and then print top 10 largest ones: 

```
%spark
val qc_1 = addrDF.groupBy("quarter").count.cache()
val qc_1_top = qc_1.orderBy(col("count").desc).limit(10)
qc_1_top.show()

Output: 
+--------------+-----+
|       quarter|count|
+--------------+-----+
|  Stadscentrum| 6473|
|        Hatert| 6388|
|        Biezen| 4945|
|Neerbosch-Oost| 4415|
|          Lent| 4333|
|     Hengstdal| 3871|
|      Heseveld| 3559|
|    Galgenveld| 3510|
|       Altrade| 3275|
|     Grootstal| 3079|
+--------------+-----+
```

For a better understanding it is possible to use _.explain()_ command to see, what is going on in the background.

## Using SQL

We can analyse the results using SQL queries to get the same results. _.explain()_ command can be used the same way to get the background information. The example of using SQL is the following: 

```
%spark
addrDF.createOrReplaceTempView("addresses")
val qc_2_top = spark.sql("SELECT quarter, count(quarter) AS qc FROM addresses GROUP BY quarter ORDER BY qc DESC LIMIT 10")
qc_2_top.show

Output: 
+--------------+----+
|       quarter|  qc|
+--------------+----+
|  Stadscentrum|6473|
|        Hatert|6388|
|        Biezen|4945|
|Neerbosch-Oost|4415|
|          Lent|4333|
|     Hengstdal|3871|
|      Heseveld|3559|
|    Galgenveld|3510|
|       Altrade|3275|
|     Grootstal|3079|
+--------------+----+
```

## Artworks

In this part we start exactly the same: loading data from a file, investigating the schema used in _.csv_ file and selecting the mostly interesting rows to investigate: "naam","locatie","latitude","longitude","bouwjaar","url". We also select artworks, which creation date is before year 1999. The commands we used are the following: 
```
%spark
val kunst = spark.read
    .format("csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("file:///opt/hadoop/share/data/kunstopstraat-kunstwerk.csv").cache()
kunst.printSchema
val kunstwerken = kunst.select("naam","locatie","latitude","longitude","bouwjaar","url").where("bouwjaar <= 1999")
``` 

In this part of the assignment we learn how to work with possible errors. Using filter function and SQL syntax we can find out that there exist artworks, which creation year is either 9999 or NULL. Such values affect mean value. If we do not exclude all the artworks with 9999 year, then we receive that mean creation year is 2043, which is in the future in current moment. Obviously, it is not correct. So it can be wise to filter this artworks out. To receive cleaner statistics I used the following command, which returns statistics of rows from _kunst_ where creationg date is known and is less than 9999 (some columns were omitted):

```
%spark
spark.sql("select * from kunst where bouwjaar < 9999 and bouwjaar is not null").describe().show()

Output:
+-------+--------------------+------------------+--------------------+--------------------+
|summary|                naam|          bouwjaar|          kunstenaar|             locatie| ...
+-------+--------------------+------------------+--------------------+--------------------+
|  count|                 344|               344|                 312|                 343| ...
|   mean|                null|1951.0203488372092|                null|                null| ...
| stddev|                null|124.34773092139906|                null|                null| ...
|    min|'Al mot ik krupe....|                12|5 m hoog kunstwer...|Aalscholverplaats...| ...
|    max|        zonder titel|              2020|       stichting NOX|d'Almarasweg (Bot...| ...
+-------+--------------------+------------------+--------------------+--------------------+
```

We save clean data to use it in the next part.

```
%spark
spark.sql("select * from kunstxy where (latitude is not null and longitude is not null) and bouwjaar < 9999")
     .write.parquet("file:///opt/hadoop/share/data/kos.parquet")
addrDF.write.parquet("file:///opt/hadoop/share/data/addr.parquet")
```

# Part B

We start with reading the files we saved in previous part. In this part we would like to analise the data we got from the databases. 

After we load the data and have a look at X and Y coordinate, then we realise, that different systems were used. To analise what artworks were created in which area we would like to convert one system to another. We install Java library [Coordinate Transformation Suite](https://github.com/orbisgis/cts) for Spark using wget to easily convert the coordinates. Library location is now in the directory __share/hadoop/common/lib__, but before Spark actually will find it, we need to restart the interpreter. 

We were provided Java/Scala [code](code.md), that transforms map coordinates to WGS:84 system, using CTS library. We start with creating a user-defined function, that that uses CT object (from the [code](code.md)) for this transformation: 

```
%spark
spark.udf.register("txyudf", (x:Float, y:Float) => CT.transformXY(x,y))
```

In the next example we tried to use _%spark.sql_ cells, which returned data in table representation. It is important to note, that coordinates are given in _latlon_ representation. If we would like to split them, then the second code snippet is used. We searched for Toernooiveld, and got the following result: 

```
%spark.sql

select street, quarter, txyudf(x,y) latlon from addr where street == "Toernooiveld"

-- Code example with split lat/lon
select street, quarter, latlon._1 as lat, latlon._2 as lon
from ( select street, quarter, txyudf(x,y) as latlon from addr where street = "Toernooiveld")
```

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/toernooiveld.png "toernooiveld")

As the second example we can take a road _Berg en Dalseweg_, that goes through 3 quarters. The pie chart below shows the per cent of road in each quarter: 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/berg.png "berg")

If we want to visualise data on map, then we need to enable 'zeppelin-leaflet' in Helium configuration. Then the new option of data visualisation appears and data is seen on the map: 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/map.png "map")

And now we would like to join two datasets. In our example we will join the results of artwork dataset against the adresses. Artwork dataset is a lot smaller and location data sometimes is missing, so it is wiser to join it with address dataset, so we will not lose the data and we just extend our knowledge of some places from address dataset. In the following example we would like to visualise where some of the artworks were created: 

```
%spark
spark.udf.register("transformLatLon", (lat:Float, lon:Float) => CT.transformLatLon(lat,lon))
```

```
%spark.sql
-- Artworks with XY coordinates
create temp view kosxy as
select naam, bouwjaar, latitude, longitude, XY._1 as x, XY._2 as y 
from ( select naam, bouwjaar, latitude, longitude, transformLatLon(latitude, longitude) as XY from kos )

-- Join addresses and artworks on location
-- drop view kosquarter;
create temp view kosquarter as
select distinct naam, quarter, first(latitude), first(longitude), min(bouwjaar) as jaar from kosxy, addr
where abs(kosxy.x - addr.x) < 10.0 and abs(kosxy.y - addr.y) < 10.0
group by naam, quarter
```

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/artworkMap.png "artworkMap")

We mapped artwork creation location with the locations from address dataset and got to know where which artwork was created. If we order artwork in ascending order by creation date, then it is possible to see, how Nijmegen was developing throughout the history. 

[< back](index.md)