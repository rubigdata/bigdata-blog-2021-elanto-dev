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