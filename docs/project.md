# Final project

[Skip to part 3](project-part3.md)

## Part 1

So, let's start with copy-paste part that is important to know what to do in DIY part. 

In this project we will work with Web Archive (WARC) file formats used a lot by CommonCrawl to store the data. We firstly develop a small data set for our analysis. We use _wget_ command to crawl the data from [course GitHub page](http://rubigdata.github.io/course/). Because I started working on assignment during the exam period - I will use the given _sh_ command: 
```
%sh
[ ! -f course.warc.gz ] && wget -r -l 3 "http://rubigdata.github.io/course/" --delete-after --no-directories --warc-file="course" || echo Whoops, most likely course.warc.gz already exists
```
We will use the implementation of the Hadoop WarcReader __HadoopConcatGZ__ created by ([this smart guy](https://www.helgeholzmann.de)) Dr. Helge Holzmann. In this part we also learn, that Zeppelin notebooks can be interactive and we use textbox to ask for file name. I guess Z stays for Zeppelin: 

```
val fname = z.textbox("Filename:")
val warcfile = s"file:///opt/hadoop/rubigdata/${fname}.warc.gz"
```

We can overwrite the default Spark Context settings for the current session using SparkConf, where we set app name, set serialiser and create class for WarcRecords. What we want to do next is to split WARC file to the records. 

```
val warcs = sc.newAPIHadoopFile(
              warcfile,
              classOf[WarcGzInputFormat],             // InputFormat
              classOf[NullWritable],                  // Key
              classOf[WarcWritable]                   // Value
    ).cache()
```

Here WARC file is the file we created earlier when got data from course GitHub page. We take the records of the file and map the, to the format, key and value pairs. From the file we got 54 records: _ warcs.count()_. 

We start with analyzing the headers. Firtly, we get all the headers of the records, then we search for the headers of WARC type request and then we get info we are interested in :
```
val whs = 
    warcs.map{ wr => wr._2 }.filter{ _.isValid() }
    .map{ _.getRecord().getHeader() }.filter{ _.getHeaderValue("WARC-Type") == "request" }
    .map{ wh => (wh.getDate(), wh.getUrl(), wh.getContentLength(), wh.getMimetype() ) }

```

We are given the example where we filter by Content-Type and we want to get only records containing text. However, for some practice I am searching for the records that do not contain text as a Content-Type: 
```
val wh = warcs
        .map{ wr => wr._2.getRecord() }.filter{ _.isHttp() }
        .map{ wr => (wr.getHeader().getUrl(),wr.getHttpHeaders().get("Content-Type")) }
        .filter{ 
            case(k,v) => v match { 
                case null => false
                case _ => !v.contains("text") }
        }
```

We could also get the bodies of the requests and it is good to add the filter, or the data is messy. I used the following command to get non-text requests: 
```
import org.apache.commons.lang3.StringUtils
val wb = warcs.
            map{ wr => wr._2.getRecord().getHttpStringBody()}.
            filter{ _.length > 0 }.
            map{ wb => StringUtils.normalizeSpace(StringUtils.substring(wb, 0, 255)) }
            .filter{ 
            case(v) => v match {
                case _ => !v.contains("text") }
            }
```

At the end of the part we give a try to Jsoup, but I excluded it from the report, because I think I will show more interesting aspects later on.

## Part 2

In this part we learn in more depth what is going on in Scala and we had a look at Scala Build Tool.


[< back](index.md)