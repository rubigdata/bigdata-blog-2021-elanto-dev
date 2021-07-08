[< Back to Part 3](project-part3.md)

```
package org.rubigdata

import org.apache.spark.SparkConf
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex

object RUBigDataApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("RUBigDataApp").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val df = spark.read
      .option("mergeSchema", "true")
      .option("enable.summary-metadata", "false")
      .option("enable.dictionary", "true")
      .option("filterPushdown", "true")
      .parquet("hdfs://gelre/user/core/cc-index-single-segment")

    df.createOrReplaceTempView("ccindex")

    // Query CC-INDEX to find Reddit WARCs.
    var redditWarcs = spark.sql("SELECT warc_filename FROM ccindex WHERE url_host_name='www.reddit.com'")
    var warcsList  = List[String]()
    val pattern = new Regex("[^/]+$")
    var broCount = 0
    var bruhCount = 0
    for(element <- redditWarcs.collect())
    {
        val location = "/single-warc-segment/" + (pattern findAllIn element.toString).mkString("")
        val warcs = sc.newAPIHadoopFile(
             location.substring(0,location.length() - 1), // Take substring, because location contains ']' at the end
             classOf[WarcGzInputFormat],             // InputFormat
             classOf[NullWritable],                  // Key
             classOf[WarcWritable]                   // Value
        )
        val wb = warcs
            .map{ wr => wr._2.getRecord().getHttpStringBody()}
            .filter{ _.length > 0 }
            .filter{
                case(v) => v match {
                case _ => v.toLowerCase().contains(" bro ") | v.toLowerCase().contains(" bruh ") }
            }
        println("BroBruh count: %s".format(wb.count())) // Print RDD's count that contain bro/bruh

        val bro = wb.filter( _.toLowerCase().contains(" bro "))
                  .map{ b => (b, countOccurrences(b, " bro "))}
                  .collect().foreach(broCount += _._2)

        val bruh = wb.filter( _.toLowerCase().contains(" bruh ") )
                  .map{ b => (b, countOccurrences(b, " bruh "))}
                  .collect().foreach(bruhCount += _._2)

    }

    println("Bro count: %s, bruh count: %s".format(broCount, bruhCount))
  }

  // Counts how many times substring tgt occured in string src
  def countOccurrences(src: String, tgt: String): Int =
    src.sliding(tgt.length).count(window => window.toLowerCase() == tgt.toLowerCase())
}
```

[< Back to main page](index.md)