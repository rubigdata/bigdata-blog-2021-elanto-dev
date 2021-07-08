# Part 3. All the magic happens here, even though I am just a muggle!

I started with the first two parts when they were released. However, had to redo them after the exam period to recal the steps I did to get the results. First two days of working on the project I spent on that and getting familiar with the Common Crawl archives and how to work with them and what they consists of. 

My first challenge was to understand how to work with CC-INDEX and what kind of information it returns to me. Even though the code was given to us, I still had to investigate what exactly I need to change to get the results that will help me to get the data I need for my idea. I had to get the names of files that I will use for data analysis later, however, when I created the query that seemed to be working - it didn't return me the result I expected. And here is the reason why... 

## Initial idea that didn't work out

My mother language is Russian, which opens for me the whole new world of opportunities. One of them: to understand Russian movies without the subtitle and to facepalm in Russian. I know that most of the movies made in Russia are pretty bad quality (not all of them!), but how bad? 

The initial idea for data analysis for the final project was gathering a data about ratings of movies made in Russia and USA and making some statistics out of them: minimum, maximum, average. Based on this statistics I wanted to know if movies made in USA are on average with higher rating or maybe I am mistaken. 

I started with query to get all the domain names containing _.ru_, because my initial target web page was _kinopoisk.ru_, where I can get the list of all movies made in selected country. The query was the following: 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/ccindex-ru-query.png "cc-index .ru query")

This query just returned all the URL's and from there I found that my targeted web page is there. I did this quite broad query because at this point of time I was not very familiar with how CC-Index works and I just kept trying baby steps to learn how it works exactly. My next query was more specific and got all WARC files we have in Common Crawl segment and URL's. However, I found out that there is no web-page I was actually interested in: web pages containing all Russian and all USA movies. At this time I realized, that I am not able to do my initial project idea on the data we had and I decided to do something less specific and different. And here we are... 

# BRUH vs. BRO

The idea is inspired by this meme: 

![alt text](https://pics.me.me/use-over-time-for-bruh-2010-1800-1850-1900-1950-58090774.png "bruh meme")

Question: do people nowadays use more "bro" or "bruh"? I guess it is time to find out! 

To answer this question I decided to use one of the most popular network of communities _reddit.com_. For the analysis I will use the data we have in the CC segment.

### Get those WARCs

To start working with WARC files I firstly had to get the WARC files. I used the similar code as provided in the picture above, but I searched by _url_host_name='www.reddit.com'_ and made a query that returns me WARC file names as a result. I got 221 WARC file names from the segment index we have. 

The file names were in the same format as on the original [index page](http://index.commoncrawl.org/CC-MAIN-2021-17/): _crawl-data/CC-MAIN-2021-17/segments/1618038066568.16/warc/CC-MAIN-20210412023359-20210412053359-00055.warc.gz_, but they were not suitable for me to get the WARC files from the server as they are located in _/single-warc-segment/_ directory. To get them I had to change the file names and I used Regex for that with the following pattern: "_[^/]+$_". This returned me string after the last slash, which I combined with the directory name. I used the resulting string to get the WARC files. 

### Caching problem

Next step was extracting one WARC file to test my code on. I used the same command we used in the first part: 

```scala
val warcs = sc.newAPIHadoopFile(
              warcfile,
              classOf[WarcGzInputFormat],     // InputFormat
              classOf[NullWritable],          // Key
              classOf[WarcWritable]           // Value
    ).cache()
```
I wrote some additional code with a filter to select RDDs which had "bruh" or "bro" as a substring in a body, but my code was failing with __Java heap space__ exception and gave me frustration for the next day, because I tried so many things to solve it. However, after venting to my friend about that, he said to remove the _.cache()_ part from the code. And the magic happened, it worked! From the [official documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/troubleshoot/memleaks002.html#:~:text=Cause%3A%20The%20detail%20message%20Java,is%20insufficient%20for%20the%20application.) I figured out that _the specified heap size is insufficient for the application_ and caching the whole WARC file can be too much of a memory use. But caching removed - problem solved!

### Finally counting

When I managed to read the file, I also checked how the filter works. What I do in the first step: I get HTTP bodies of WARC file records, I filter out empty bodies and then I filter out RDDs containing no "bro" or "bruh": 

```scala
val wb = warcs
        .map{ wr => wr._2.getRecord().getHttpStringBody()}
        .filter{ _.length > 0 }
        .filter{
            case(v) => v match {
                case _ => v.toLowerCase().contains(" bro ") | v.toLowerCase().contains(" bruh ") }
        	    }
```
Initially I used _.contains("bro")_, but after printing out the first RDD body I found out, that such things as "Browser" also contain "bro", so I surrounded both substrings with spaces. 

After RDDs not countaining these words are filtered out, there is less data to work with. Next step is to count how many times the words appeared in the RDDs and they were divided into two separate works. Firstly I counted "bro": from RDD's we had I filtered out the ones containing "bro", then counted the occurences of the substring in RDD text using [this function I found on StackOverflow](https://stackoverflow.com/questions/43323530/finding-how-many-times-a-given-string-is-a-substring-of-another-string-using-sca/43324063), which I slightly modified and after that I increase the general _broCount_ by the number of substrings in RDD. _broCount_ is a variable, which I assign 0 before counting. For "bruh" I did exactly the same operations, but in this case _bruhCount_ is increased. 

```scala
val bro = wb.filter( _.toLowerCase().contains(" bro "))
            .map{ b => (b, countOccurrences(b, " bro "))}
            .collect().foreach(broCount += _._2)
```

Even though the final code contains only 3 lines in the counter, but I spent quite some time on understanding what data types do we get and how to correctly work with them. Firstly, I tried _.reduceByKey((x,y) => (x + y))_ after the map, because I thought that it will count the amount of "bro/bruh" and reduce them to one key-value pair, but it didn't work out and resulted in empty array. Secondly, I also tried _.foreach(broCount += \_.\_2)_ on RDDs, but this code didn't even compile. I tried some other approaches as well, but this resulted in the code described above. [_.collect()_](https://sparkbyexamples.com/spark/spark-dataframe-collect/) function retrieves data from RDDs to Array[row] and _.foreach()_ function can be applied correctly on the data. 

In the end I print out _broCount_ and _bruhCount_. The result of this program on one WARC file is the following: 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/brobruh-one-warc.png "bro/bruh one WARC")

As my program succeeded to give me results with one WARC file, I decided: 

![alt text](https://media.tenor.com/images/29032afbeee3a5ecf573f03871c08ffb/tenor.gif)

... and ran for-loop on all the WARC files I got from the first query and ran my code to analyse each of them on the silver queue. I ran my job at around 00:40 on 08-07-2021 and currently at 17:04 on 08-07-2021 I am still waiting. 

### Final results

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/job-time.png "bro/bruh one WARC")

So, after running for 20.3 hours my program returned me the result and, surprisingly, didn't fail. The main question of this project was bro or bruh. And the answer is... BRO! 

![alt text](https://rubigdata.github.io/bigdata-blog-2021-elanto-dev/images/bruhbro-final.png "bro/bruh one WARC")

The statistics showed that bro is used almost 32 times more. This can be caused, because bro has been longer in the Internet culture and used more often. Moreover, bruh is more situational, so it is used less, while bro can be used to every person on the internet.

Hereby I think I succeeded to reach the goal I made in the beginning of the project and answered the question by big data analysis using Spark technology. The full final code of the program is accessible [here](project-code.md). It is not the prettiest and might be improved, but it helps to answer the question posted in the beginning of this small research.