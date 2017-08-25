library(readxl)
library(readr)
library(xlsx)
library(dplyr)
library(clValid)
library(ggplot2)
library(RColorBrewer)
library(ggrepel)
library(GGally)
library(devtools)
library(purrr)
library(googleVis)

library(xlsx)
# source_url('https://gist.githubusercontent.com/fawda123/5281518/raw/a7194a748cda5a8d2ea83f87e4a59496ee5e0953/plot_qual.r')
source('C:/Users/2187/Desktop/大數據/數據中心業務/重點市場篩選指標/2018市場篩選/RCA1.r',
       encoding = 'UTF-8')
source('//172.20.23.190/ds/Scripts/equalizr.r', encoding = 'UTF-8')

# Import Data
all_var <- read_csv('C:/Users/2187/Desktop/dataset/new model/市研處model/model_data_all.csv', col_types='ccddddddddddddd')
names(all_var)[1] <- 'reporter' 

temp <- read_csv('C:/Users/2187/Desktop/大數據/數據中心業務/重點市場篩選指標/2018市場篩選/stacked_pred.csv' )
temp$country <- equalize(temp$country)

# Compute Aggregate NRCA and Market Share
join.perc <- inner_join(ctgy.s, ct.perc, by = 'category')
nrca <- {
  ungroup(join.perc) %>%
    mutate(nrca.weighted = nrca * percentage,
           share.g.weighted = share_growth * percentage) %>%
    group_by(reporter) %>%
    summarise(nrca.ttl = sum(nrca.weighted),
              share.ttl = sum(share.g.weighted))
}

#use real export value
#================================================================
#2. export
#===讀取資料用===#funtion
readMonthlyData <- function(year, month, currency = c('usd', 'twd')){
  
  monthly <- read_delim(paste0('//172.26.1.102/dstore/Projects/data/mof-export-', currency, '/',year,'-',month,'.tsv'),
                        delim = '\t', col_types = 'cc_c____n', locale = locale(encoding = "UTF-8"))
  monthly <- monthly[,c(3,1,2,4)]
  colnames(monthly) <- c('country','code','product','value')
  monthly$code <- str_pad(monthly$code, 11, pad = "0")
  
  # monthly <- monthly %>% filter(code %in% product_list)
  #monthly <- monthly[grepl(product_list, monthly$code),]
  
  monthly$year <- year
  monthly$month <- month
  monthly
}

selectData <- function(start_yr, end_yr, start_mon, end_mon, currency = c('us', 'nt')){
  
  file_list <- c()
  
  for(year in start_yr:end_yr){
    for(month in start_mon:end_mon){
      month <- sprintf('%02d',month)
      
      file_name <- (paste0(year,'-',month))
      file_list <- c(file_list, file_name)
    }
  }
  years <- substr(file_list, 1, 4)      #篩出月份與年份
  months <- substr(file_list, 6, 7)
  
  ls <- map2(years, months, function(x, y) {readMonthlyData(year = x, month = y, currency = currency)})
  df <- purrr::reduce(ls, rbind)
  df
}

#手動輸入年份與月份;將會計算設定年份與前兩年之資料(共三年)
end_yr <- 2017 ; start_yr <- 2016
start_mon <- 1 ; end_mon <- 6
currency <-'usd'

#讀取海關資料
custom <- selectData(start_yr,end_yr,start_mon,end_mon,currency)
export <- group_by(custom, year, country) %>% summarise(export=sum(value))
export <- export %>% spread(year, export)
export$g17 <- (export$`2017` - export$`2016`)/ (export$`2016`)*100

country_map <- read_csv('//172.20.23.190/ds/Raw Data/2016大數爬蟲案/data/ITC HS6/itc_df_complete.csv',
                        col_types = c('_cc'))
export <- left_join(export, country_map, by=c('country'='countryName')) %>% select(c(5,2:4))
names(export) <- c('country','export16','export17','growth17')
export$country<- equalize(export$country)

#compute export value in 2018 use real 2016 value *2017 predicted.g*2018 predicted.g
export_temp <- filter(all_var, year==2016)%>%select(reporter, tw_ex)%>% 
  right_join(temp %>% filter(year=='2017/1/1') %>% select(country ,ex_growth_nnstack), by=c('reporter'='country') )%>%
  right_join(temp %>% filter(year=='2018/1/1') %>% select(country ,ex_growth_nnstack), by=c('reporter'='country') )
names(export_temp) <- c('reporter', 'export.value', 'g17', 'g18')
export_value <- export_temp %>% mutate(export_v18=export.value*(1+g17/100)*(1+g18/100)) %>% select(reporter, export_v18)

#join company numbers
company_n <- read_csv('//172.20.23.190/ds/出口國/各國廠商數/company_n_country.csv', col_types='cdd')
company_n$country <- equalize(company_n$country)

#risk
risk <- read_excel('C:/Users/2187/Desktop/dataset/new model/市研處model/country-risk-ratings-q1-2017-mar17.xlsx', col_types=c('text', 'text', 'blank'))
names(risk) <- c('country', 'risk')
risk$country <- equalize(risk$country)

# Join Data Frames
df <- left_join(temp%>% filter(year=='2018/1/1'), nrca, by = c('country'='reporter'))%>%left_join(export_value, by=c('country'='reporter'))
names(df)[2] <- "reporter"
df <- left_join(df, company_n, by = c('reporter'='country')) %>% left_join(risk, by = c('reporter'='country'))

#transfer risk to numbers
lut <- c("AA" = 100,
             "A" = 100 / 5 * 4,
             "BB" = 100 / 5 * 3,
             "B" = 100 / 5 * 2,
             "C" = 100 / 5 * 1,
             "D" = 0,
             "N/A" = NA)
df$risk <- lut[df$risk]


df.n <- df[complete.cases(df), ]  

any(is.na(df.n))


#版本一
df.n <- df.n %>% select(c(2,3,16,19,22))

#版本二
df.n <- df.n %>% select(c(2,16,19,22))
# Save row names
#c.names <- df.n[df.n$reporter!='Venezuela'&df.n$reporter!='Lao People\'s Dem. Rep.',]$reporter
c.names <- df.n$reporter
# Normalize
df.n <- data.frame(lapply(df.n[-1], function(x) {(x - min(x))/(max(x) - min(x))}))
#to avoid -Inf by taking log()
#df.n[df.n$gdp_capita_growth==0,]$gdp_capita_growth <- 0.00000000000000000000000000000001
df.n[df.n$export_v18==0,]$export_v18 <- 0.0000000000000000000000000000000000001
#df.n[df.n$company_n==0,]$company_n <- 0.0000000000000000000000000000000000001

df.n['export_v18'] <- data.frame(lapply(df.n['export_v18'], function(x) {log(x)}))
#df.n$nrca.ttl <- df.n$nrca.ttl %>% `^` (2)
#df.n <- df.n[!is.infinite(df.n$export_v18), ]  
#df.n <- df.n[!is.infinite(df.n$company_n), ]  

# Clustering
rownames(df.n) <- c.names
#拿掉寮國、委內瑞拉
#df.n <- df.n[rownames(df.n) !='China'&rownames(df.n) != 'Venezuela'&rownames(df.n) != 'Lao People\'s Dem. Rep.'&rownames(df.n) != 'Algeria'&rownames(df.n) != 'El Salvador'&rownames(df.n) != 'Panama', ]
df.n <- df.n[rownames(df.n) != 'Venezuela'&rownames(df.n) != 'Lao People\'s Dem. Rep.'&rownames(df.n) != 'Algeria'&rownames(df.n) != 'El Salvador'&rownames(df.n) != 'Panama', ]

dist <- dist(df.n, 'manhattan')
tree <- hclust(dist, 'complete')

# Find n that Maximizes Dunn's Index
# df.dunn <- data.frame(nc = 2:10, dunn = rep(NA, 9))
# for (x in 2:10) {
#   cut.tree <- cutree(tree, k = x)
#   df.dunn[df.dunn$nc == x, 'dunn'] <- dunn(distance = dist, clusters = cut.tree)
# }

# Dunn's Index Is Maximized When n = 7
# nc <- df.dunn[df.dunn$dunn == max(df.dunn$dunn), ]$nc
nc <- 4
cut.tree <- cutree(tree, k = nc)

# Plot Dendrogram
plotDendr <- function() {
  par(cex = 0.6)
  plot(tree, hang = -1)
  rect.hclust(tree, k = nc, border = 'red')
}

# Add Labels to Original Data Frame
df.grouped <- df %>% filter(year=='2018/1/1') %>% mutate(label = cut.tree[reporter])
df.grouped <- select(df.grouped, c(2,23,16,19,3:5,17:18,20:22))%>% arrange(label, desc(ex_growth_nnstack))

# Insert row for "Bangladesh" "Iran"       "Kenya"      "Myanmar" 
# (the latter three were dropped because lack of NRCA)
#no.nrca <- df[which(is.na(df$nrca.ttl)),]$country %>% unique()
#three <- {
#  left_join(temp1, temp2, by = 'reporter') %>%
#    filter(reporter == 'Iran' | reporter == 'Bangladesh' | reporter == 'Myanmar' |
#           reporter == 'Kenya') %>%
#    mutate(nrca.ttl = NA, share.ttl = NA, label = NA)
#}
#df.grouped <- rbind(df.grouped, three)

# Translate scores back to risk ratings
df.grouped$risk <- as.character(df.grouped$risk)
lut2 <- c('100' = "AA",
          '80' = "A",
          '60' = "BB",
          '40' = "B",
          '20' = "C", 
          '0' = "D")
df.grouped$risk <- lut2[df.grouped$risk]


# Round down to the first place
df.grouped$ex_growth_nnstack <- round(df.grouped$ex_growth_nnstack, 2)
df.grouped$export_v18 <- round(df.grouped$export_v18, 2)
df.grouped$gdp_growth <- round(df.grouped$gdp_growth, 2)
df.grouped$gdp_capita_growth <- round(df.grouped$gdp_capita_growth, 2)
df.grouped$im_growth <- round(df.grouped$im_growth, 2)
df.grouped$nrca.ttl <- round(df.grouped$nrca.ttl, 2)
df.grouped$share.ttl <- round(df.grouped$share.ttl, 2)

# Join weighted tariff
tariff <- read_csv('//172.20.23.190/ds/出口市場篩選/data/tariff_itc.csv')
names(tariff) <- c('reporter', 'weighted.tariff')
tariff$reporter <- equalize(tariff$reporter)
df.grouped <- left_join(df.grouped, tariff, by = 'reporter')

# Fix tariff for the seven reporters
df.grouped[df.grouped$reporter %in% c('Panama', 'Guatemala', 'Nicaragua', 'El Salvador',
                              'Honduras', 'New Zealand', 'Singapore'),'weighted.tariff'] <- 0

#add country's not in itc
tariff1 <- read_csv('//172.20.23.190/ds/出口市場篩選/data/tariff_new.csv')
names(tariff1) <- c('reporter', 'weighted.tariff')
lack.tariff <- df.grouped[which(is.na(df.grouped$weighted.tariff)), 'reporter'][[1]] %>% intersect(tariff1$reporter)

for (i in 1:length(lack.tariff)){
  df.grouped[df.grouped$reporter==lack.tariff[i], 'weighted.tariff'] <- tariff1[tariff1$reporter==lack.tariff[i], 'weighted.tariff'] 
  
} 

# Join rsd data frame
#all_var <- all_var[complete.cases(all_var), ]  
rsd.df <- 
  arrange(all_var,reporter) %>%
  group_by(reporter)%>%
  summarise(exchange_rate_sd=sd(`exchange rate`))

rsd.df$reporter <- equalize(rsd.df$reporter)

#add population, import
new_var <- filter(all_var, year==2018) %>% select(reporter, FDI, import, population) 
df.grouped <- left_join(df.grouped, rsd.df, by = 'reporter')%>%left_join(new_var, by = 'reporter')

#開發程度
development <- read_excel('C:/Users/2187/Desktop/dataset/new model/市研處model/development.xlsx', col_types=c('text', 'blank', 'blank', 'text', 'blank', 'blank'))
names(development) <- c('reporter', 'income group')
development$reporter <- equalize(development$reporter)

# Join Herfindahl data frame
df.grouped <- left_join(df.grouped, herf, by = 'reporter') %>% left_join(development, by='reporter')


names(df.grouped)[4] <- 'export_value' 
names(df.grouped)[3] <- 'export_growth' 

#mapping area
country.code <- read_csv("C:/Users/2187/Desktop/dataset/2017data/iso_3166_countries_rigions.csv")
country.code$name <- equalize(country.code$name)
df.grouped <- left_join(df.grouped, country.code %>% select(1,6), by = c('reporter'='name'))
df.grouped$label <- df.grouped$label %>% as.character()

# Translate group numbers to letters
lut <- c('1' = 'A',
         '2' = 'B',
         '3' = 'C',
         '4' = 'D'
         )
df.grouped$label <- lut[df.grouped$label]
df.grouped[is.na(df.grouped$label),]$label <- 'E'
df.grouped <- df.grouped[,c(1:2,20,19,3:7,15:17,8:9, 18, 10:14)]
#df.grouped <- left_join(df.grouped, export, by=c('reporter'='country'))
#df.grouped[,c(6,10,11,12,16,17)] <- format(df.grouped[,c(6,10,11,12,16,17)], big.mark=',', big.interval=3L)
#df.grouped[,c(6,10,11,12,16,17)] <- map(df.grouped[,c(6,10,11,12,16,17)],as.numeric)

#df <- left_join(df.grouped, export_value, by=c('reporter'))
#write.table(df,'C:/Users/2187/Desktop/大數據/數據中心業務/重點市場篩選指標/2018市場篩選/df.csv', sep = ',', row.names=F)



# Cluster reporters based on Herfindahl index
clusterHerf <- function() {
  xx <- df.grouped[, c('reporter', 'herf')]
  xx <- xx[complete.cases(xx), ]
  xx <- xx[!is.nan(xx), ]
  dist <- dist(xx$herf, method = 'euclidean')
  hc <- hclust(dist, method = 'complete')
  plot(hc, labels = substr(xx$reporter, 1, 10), hang = -1)
}

# Output worksheet version A (default, by cluster)
outputVerA <- function() {
  arrange(df.grouped, label) %>%
    write.table('C:/Users/2187/Desktop/大數據/數據中心業務/重點市場篩選指標/2018市場篩選/market selection/data/outputA.csv', sep = ',')
}

# Compute influence of weight on ranking
computeRank <- function(w, group = FALSE) {
  if (group == TRUE) {
    mutate(df.grouped,
           ex.norm = (export_value - min(export_value)) / (max(export_value) - min(export_value)),
           grate.norm = (export_growth - min(export_growth)) / (max(export_growth) - min(export_growth)),
           score = w * ex.norm + (1 - w) * grate.norm) %>%
      group_by(label) %>%
      arrange(label, desc(score)) %>%
      mutate(rank = row_number()) %>%
      select(label, reporter, rank) %>%
      return()
  } else {
  mutate(df.grouped,
         ex.norm = (export_value - min(export_value)) / (max(export_value) - min(export_value)),
         grate.norm = (export_growth - min(export_growth)) / (max(export_growth) - min(export_growth)),
         score = w * ex.norm + (1 - w) * grate.norm) %>%
    arrange(desc(score)) %>%
    mutate(rank = row_number()) %>%
    select(reporter, rank)
  }
}
compareRank <- function() {
  d <- computeRank(0.6)
  names(d) <- c('reporter', 'rank0.6')
  for (w in seq(0.65, 0.95, 0.05)) {
    d.new <- computeRank(w)
    names(d.new) <- c('reporter', paste0('rank', as.character(w)))
    d <- left_join(d, d.new, by = 'reporter')
  }
  write.table(arrange(d, rank0.7), 'compareRank', sep = ',')
}
compareRank.gp <- function() {
  d <- computeRank(0.6, group = TRUE)
  names(d) <- c('label', 'reporter', 'rank0.6')
  for (w in seq(0.65, 0.95, 0.05)) {
    d.new <- computeRank(w, group = TRUE)
    names(d.new) <- c('label', 'reporter', paste0('rank', as.character(w)))
    d <- left_join(d, d.new, by = c('label', 'reporter'))
  }
  write.table(arrange(d, label, rank0.7), 'compareRankGp', sep = ',')
}

# Output worksheet version B (ratio export to growth = 6:4, by cluster)
outputVerB <- function() {
  df <- mutate(df.grouped,
         ex.log = log(export_value),
         ex.norm = (ex.log - min(ex.log)) / (max(ex.log) - min(ex.log)),
         grate.norm = (export_growth - min(export_growth)) / (max(export_growth) - min(export_growth)),
         score = (.7 * ex.norm + .3 * grate.norm)*100%>%round(2) )%>%
    arrange(label, desc(score)) %>%
    select(c(1:3,24,4:21))
  df[,c(4,11,16,20:21)] <- sapply(df[,c(4,11,16,20:21)],function(df){round(df,2)})
  df[,c(7,11,12,13,17,18)] <- format(df[,c(7,11,12,13,17,18)], big.mark=',', big.interval=3L)
  
  write.table(df, 'C:/Users/2187/Desktop/大數據/數據中心業務/重點市場篩選指標/2018市場篩選/target/data/output1.csv', sep=',',row.names=F)
}

# Output worksheet version C (ratio export to growth = 6:4, no cluster)
outputVerC <- function() {
  mutate(df.grouped,
         ex.norm = (export_value - min(export_value)) / (max(export_value) - min(export_value)),
         grate.norm = (export_growth - min(export_growth)) / (max(export_growth) - min(export_growth)),
         score = .6 * ex.norm + .4 * grate.norm) %>%
    arrange(desc(score)) %>%
    select(c(23,1:20)) %>%
    write.table('C:/Users/2187/Desktop/大數據/數據中心業務/重點市場篩選指標/2018市場篩選/market selection/data/outputC.csv', sep = ',')
}

#join industry with outputX
outputC <- read_csv('C:/Users/2187/Desktop/大數據/數據中心業務/重點市場篩選指標/2018市場篩選/outputC.csv')
country_name <- read_csv(file = '//172.20.23.190/ds/Raw Data/2016大數爬蟲案/data/ITC HS6/itc_df_complete.csv',
                         col_types = c('_cc'))
country_name$itc_name <- equalize(country_name$itc_name)
country_name <- country_name %>% distinct()
industry <- read_excel('//172.20.23.190/ds/出口市場篩選/2018重點市場/重點市場產業表.xlsx')
output <- left_join(outputC, country_name, by=c('reporter'='itc_name'))%>%left_join(industry, by=c('countryName'='country'))
write.table(output,'C:/Users/2187/Desktop/大數據/數據中心業務/重點市場篩選指標/2018市場篩選/outputC_in.csv', sep = ',', row.names=F)


#_______join taitra data____________
taitra <- read_excel('//172.26.1.102/dstore/交換檔案/20170717-目標市場篩選補充資料/to-vicky-20170719/variables_group_by_country_20170719.xls')
outputB<- read_excel('C:/Users/2187/Desktop/大數據/數據中心業務/重點市場篩選指標/2018市場篩選/前版本資料/target_ch1/data/outputB.xlsx')
country_name <- read_csv(file = '//172.20.23.190/ds/Raw Data/2016大數爬蟲案/data/ITC HS6/itc_df_complete.csv',
                         col_types = c('_cc'))
country_name$itc_name <- equalize(country_name$itc_name)

output <- left_join(outputB, left_join(taitra, country_name, by=c('ds_countryNameFix'='countryName')), by=c('國家'='itc_name'))
names(output)[23:30] <- c("商機總量2015",               "商機總量2016",              
                          "觀展公司數2015", "觀展公司數2016",
                          "海外觀展公司數2015",　         "海外觀展公司數2016",        
                          "TT商機數2016",             "TT流量201609至201706") 
output <- output[-c(85:96),c(22,2:21,23:40)]
names(output)[1] <- '國家'
output <- unique(output)

#mapping area
country_area <- read_csv('C:/Users/2187/Documents/GitHub/shiny/industry_mk/data/ds_export_markets.csv')

output <- left_join(output, country_area[,c(3,5,7,9)],by=c('國家'='export_countryName'))
output <- output[,c(1:2,40,4:39,41:42)]
names(output)[3] <- '地區'
write.xlsx(as.data.frame(output),'C:/Users/2187/Documents/GitHub/shiny/industry_mk/data/outputD.xlsx', row.names=F)
