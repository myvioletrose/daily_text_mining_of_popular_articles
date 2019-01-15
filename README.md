######################

This is a simple text mining script hitting the story dim inside Google Big Query. The script is set up to run daily using Windows Task Scheduler on my local machine. The script extracts key words (unnest_tokens) from the summary of most popular articles read in the past 48 hours on Bloomberg.com. We visualize the text by channels (such as pursuits, markets, technology, businessweek, etc.). Subsequently, we send off the charts (word cloud, bar chart) to a list of email recipients. We want to know the top 10 articles by channel in the past 48 hours, and we also want to visualize their differences in words, e.g. most popular technology talk is all about "bitcoin", whereas "brexit" is hottest in the businessweek and markets in the past 48 hours. 

This is just a toy example as a proof of concept preceding a much more complicated text mining, ETL project later.
