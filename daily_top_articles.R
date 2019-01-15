# https://www.youtube.com/watch?v=UDKy5_SQy2o
# https://github.com/trinker/Make_Task

library(bigrquery)
library(rmarkdown)
library(knitr)
library(mailR)
library(lubridate)
library(dplyr)
library(ggplot2)

run.time <- date(now())

setwd("C:/Users/traveler/Desktop/R and Windows Task Scheduler")

files <- grep(pattern = "top_articles_output|dist_published_date|dist_primary_site|top_10_words_from_each_site|wordCloud_top_articles", dir(), value = T)

unlink(files, recursive = T, force = F)

projectId <- "media-data-science"  # this is the Project ID

sql <- "
select s.primary_site as primary_site
, s.published_at as published_at
, s.headline as headline
, s.summary as summary
, a.read as reader
, a.rank as rank
, row_number() over (order by a.read desc) as row_num

from (
select story_id
, read
, row_number() over (order by read desc) as rank

from (

select hits.customDimensions.value as story_id
, count(distinct fullVisitorId) as read
from TABLE_DATE_RANGE([calcium-land-150922:132466937.ga_sessions_], 
DATE_ADD(CURRENT_DATE(), -2, 'DAY'), 
DATE_ADD(CURRENT_DATE(), 0, 'DAY'))
--looking for past 2 days
where hits.type = 'PAGE'
and hits.customDimensions.index = 7
group by 1

) x
) a

join [calcium-land-150922:datalake.story_metadata] s on a.story_id = s.id
--top 100 articles by number of unique visitors
--not all stories can be found (or updated yet) in data lake so there is a discrepancy between rank and row_num
order by 6
limit 500
"
# write query and save result
data <- query_exec(query = sql,
                   project = projectId,
                   useLegacySql = T)

# get published date and primary site distributions
dist.published.date <- as.data.frame(ftable(date(data$published_at))) %>%
        mutate(percent = round((Freq / nrow(data)) * 100, 1),
               month = paste0(lubridate::year(Var1), "-", lubridate::month(Var1)))

chart1 <- dist.published.date %>%
        ggplot(aes(Var1, Freq)) +
        geom_bar(stat = "identity", aes(fill = month)) +
        # facet_wrap(~ month, scales = "free", ncol = 4) + 
        theme(strip.text = element_text(size = 7.5)) +
        ggtitle("Distribution of Popular Articles by Published Dates") + 
        labs(x = "Published Date", y = "Number of Articles\n divided by 5 to get (%)") +
        scale_fill_discrete(guide = F) + 
        coord_flip() +
        theme(legend.position = "Top", 
              axis.text.x = element_text(angle = 90, hjust = 1),
              plot.title = element_text(hjust = 0.5)) 

ggsave(filename = paste0("dist_published_date_", run.time, ".png", sep = ""),
       plot = chart1, 
       width = 11.7,
       height = 8.3,
       units = "in")  # save in standard A4 size

dist.primary_site <- as.data.frame(ftable(data$primary_site)) %>%
        arrange(-Freq) %>%
        mutate(percent = round((Freq / nrow(data)) * 100, 1))


### Writing back to BigQuery

# insert_upload_job(projectId,  # Project ID 
#                   "Jim_Work",  # Dataset 
#                   "test_data",  # Table Name
#                   data  # R object
# )


#################################################################
## write output

write.table(data,
            file = paste0("top_articles_output_", run.time, ".csv", sep = ""),
            append = F, row.names = F, col.names = T,
            sep = ',')

# write.table(dist.published.date, 
#             file = paste0("dist_published_date_", run.time, ".csv", sep = ""), 
#             append = F, row.names = F, col.names = T,
#             sep = ',')

write.table(dist.primary_site, 
            file = paste0("dist_primary_site_", run.time, ".csv", sep = ""), 
            append = F, row.names = F, col.names = T,
            sep = ',')


#####################################################################
#### start text mining here 
## look at summary 

library(tidytext)

# clean text first #
clean.text = function(x)
{
        # tolower
        x = tolower(x)
        # remove rt
        x = gsub("rt", "", x)
        # remove at
        x = gsub("@\\w+", "", x)
        # remove punctuation
        x = gsub("[[:punct:]]", "", x)
        # remove numbers
        x = gsub("[[:digit:]]", "", x)
        # remove links http
        x = gsub("http\\w+", "", x)
        # remove tabs
        x = gsub("[ |\t]{2,}", "", x)
        # remove blank spaces at the beginning
        x = gsub("^ ", "", x)
        # remove blank spaces at the end
        x = gsub(" $", "", x)
        return(x)
}

tidyData <- data %>%
        subset(., !is.na(summary)) %>%
        mutate(clean_summary = clean.text(summary)) %>%
        select(-summary) %>% 
        unnest_tokens(output = token, 
                      input = clean_summary) %>%
        filter(token != "bloomberg")

# remove stopwords #
tidyData <- tidyData %>%
        anti_join(stop_words, by = c("token" = "word")) 

###############################################
### count tokens and group by primary_site ###
## save the top 10 words from each site ##
tidyData2 <- tidyData %>%
        group_by(primary_site) %>%
        count(token, sort = T) %>%
        # slice(1:10) %>%
        ungroup %>%
        arrange(., primary_site, desc(n))

# tidyData2 %>%
#         group_by(primary_site) %>%
#         slice(1:10) %>%
#         write.table(., 
#                     file = paste0("top_10_words_from_each_site_", run.time, ".csv", sep = ""), 
#                     append = F, row.names = F, col.names = T,
#                     sep = ',') 

chart2 <- tidyData2 %>%
        group_by(primary_site) %>%
        slice(1:10) %>%
        # top_n(10) %>%  # return more than 10 rows when there is a tie
        ungroup %>%
        mutate(token = as.character(reorder(token, n))) %>%
        as.data.frame(stringsAsFactors = F) %>%
        # still not sorting properly after converting factor to character
        # ggplot(aes(x = reorder(as.character(token), n), y = n)) +
        ggplot(aes(x = reorder(token, n), y = n)) +
        geom_bar(stat = "identity", aes(fill = primary_site)) +
        facet_wrap(~ primary_site, scales = "free", ncol = 4) + 
        theme(strip.text = element_text(size = 7.5)) +
        ggtitle("Top 10 Words from each Primary Site") + 
        labs(x = "Top 10 Words", y = "") +
        # scale_fill_discrete(guide = F) + 
        coord_flip() +
        theme(legend.position = "Top", 
              axis.text.x = element_text(angle = 90, hjust = 1),
              plot.title = element_text(hjust = 0.5)) 

ggsave(filename = paste0("top_10_words_from_each_site_", run.time, ".png", sep = ""),
       plot = chart2, 
       width = 11.7,
       height = 8.3,
       units = "in")  # save in standard A4 size


##################################################################################
################################### word cloud ###################################
##################################################################################
library(wordcloud)

windows()
set.seed(1234)

# filter out 'bloomberg'
tidyData %>%
        count(token, sort = T) -> wc

with(wc, 
     wordcloud(token, n, 
               min.freq = 5, 
               colors = c("#00B2FF", "#FF0099"))
)

savePlot(file = paste0("wordCloud_top_articles_", run.time, ".png"), type = "png")

dev.off()



# see https://github.com/rpremraj/mailR
sender <- "myvioletrose@gmail.com" 
recipients <- c("myvioletrose@gmail.com") 
# username <- "xyz"
# password <- "xyz"

email <- mailR::send.mail(from = sender,
                   to = recipients,
                   subject = "Top 500 Bb Reads in Past 48 Hours",
                   body = "FYI",
                   smtp = list(host.name = "smtp.gmail.com", 
                               port = 465, 
                               user.name = username,
                               passwd = password,
                               ssl = T),
                   authenticate = T,
                   send = T,
                   attach.files = c(grep(pattern = "top_articles_output|dist_published_date|dist_primary_site|top_10_words_from_each_site|wordCloud_top_articles", dir(), value = T)))


# #################################
# # schedule a R script using CLI #
# #################################
# 
# recurrence <- "once"
# task_name <- "MyTask"
# bat_loc <- "C:\\Users\\Tyler\\Desktop\\Make_Task\\task.bat"
# time <- "00:21"
# 
# system(sprintf("schtasks /create /sc %s /tn %s /tr \"%s\" /st %s", recurrence, task_name, bat_loc, time))
# 
# 
# ## Additional arguments
# browseURL("https://msdn.microsoft.com/en-us/library/windows/desktop/bb736357%28v=vs.85%29.aspx")
# 
# # open the Windows Task Scheduler
# system("control schedtasks")



