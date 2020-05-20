library(readr)
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(readxl)
library(xlsx)
library(data.table)

source('/process_export_rev.R', encoding = 'UTF-8')
source('/process_import_rev.R', encoding = 'UTF-8')


#Read data
#================================================================
#1.import
# Setting Java heap to increase memory
options(java.parameters = "-Xmx4000m") 

# 載入2012-2016年UN Comtrade各國進出口資料
#出口資料
rawData <- read.csv('/uncomtrade/annual_reduced/with-weight-kg/un-export-hs6-2012-2016_v2.csv', 
                      colClasses = c(rep('numeric', 2),'character',rep('numeric',10)))[,c(1:3,4:13)]

#進口資料
rawData <- fread('/uncomtrade/annual_reduced/with-weight-kg/update/un-import-hs6-2011-2016.csv', 
                colClasses = c(rep('numeric', 2),'character',rep('numeric',12)))[,c(1:3,6:15)]

rawData$reporter_code <- str_pad(rawData$reporter_code, 3, pad = '0')
rawData$partner_code <- str_pad(rawData$partner_code, 3, pad = '0')
rawData$hscode6 <- str_pad(rawData$hscode6, 6, pad = '0')

#mapping for reporter & partner
country_name <- read_csv(file = '/Raw Data/2016大數爬蟲案/data/ITC HS6/itc_df_complete.csv',
                         col_types = c('c_c')) %>% unique()

#by country
importData <- merge(x = rawData, y = country_name, by.x = 'reporter_code', by.y = 'ds_code')
colnames(importData)[14] <- 'reporter'
importData <- merge(x = importData, y = country_name, by.x = 'partner_code', by.y = 'ds_code')
colnames(importData)[15] <- 'partner'
importData <- filter(importData, partner != '所有國家')[c(14:15, 3:13)]

#補值
country_name <- unique(importData$reporter)
countries <- c()

for(i in 1:length(country_name)){
  tmp <- importData %>% filter(reporter == country_name[i])
  
  if(sum(is.na(tmp$v2016)) == nrow(tmp)){
    importData[importData$reporter == country_name[i], 'v2016'] <- importData[importData$reporter == country_name[i], 'v2015']
    importData[importData$reporter == country_name[i], 'wkg2016'] <- importData[importData$reporter == country_name[i], 'wkg2015']
    
    countries <- c(countries, country_name[i])
  }
  rm(tmp)
}


#================================================================
#2. export
#===讀取資料用===#funtion
readMonthlyData <- function(year, month, currency = c('usd', 'twd')){
  
  monthly <- fread(paste0('//172.26.1.102/dstore/Projects/data/mof-export-', currency, '/',year,'-',month,'.tsv'), 
                   colClasses =  c(rep('character',6),'numeric','character','numeric'), 
                   select = c(1,2,4,7,8,9), encoding = 'UTF-8')
  names(monthly) <- c('code','product','country','weight','unit','value')
  monthly[monthly$unit=='TNE ','weight'] <- monthly[monthly$unit=='TNE ','weight'] *1000
  monthly$code <- str_pad(monthly$code, 11, pad = "0")
  monthly$code <- as.character(monthly$code)
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
end_yr <- 2016 ; start_yr <- 2012
start_mon <- 1 ; end_mon <- 12
currency <-'usd'

#讀取海關資料
custom <- selectData(start_yr,end_yr,start_mon,end_mon,currency)

#================================================================
#3. company
rawData <- read.csv('/Raw Data/kmg/kmg_HS6Country_md5.csv')
rawData2 <- read.csv('/ds/Raw Data/kmg/kmg_hscode.csv')

rawData[is.na(rawData)] <- 13
rawData2[is.na(rawData2)] <- 13
com_df <- rawData %>% filter(MONTH == 13, YEAR == 2016, EXPORT != 0 )
com_df$BAN_REAL <- as.character(com_df$BAN_REAL) 
com_df$COUNTRY <- as.character(com_df$COUNTRY) 
com_df2 <- rawData2 %>% filter(MONTH == 13, YEAR == 2016, EXPORT != 0 )
com_df2$BAN_REAL <- as.character(com_df2$BAN_REAL) 

#mapping country code to Chinese name
country_map <- read_csv('/出口市場篩選/產業別出口市場/ds_export_markets.csv',
                        col_types = c('ccc______'))
com_df <- left_join(com_df, country_map, by=c('COUNTRY'='mof_enCode'))[,c(1:3, 10,5:8 )]

#process export number from 10WUSD to USD
com_df$EXPORT <- com_df$EXPORT*100000+50000
com_df <- com_df[,c(2,3,4,7)] %>% arrange(desc(EXPORT), BAN_REAL, HSCODE)
names(com_df) <- c("BAN_REAL", "code", "COUNTRY","EXPORT")
com_df$code <- formatC(com_df$code, width = 6, flag = 0)

#產業分析資料
tax <- read.csv('/Raw Data/kmg/taxdata0119.csv', header = T, sep = ",")
tax$BAN_REAL <- formatC(tax$BAN_REAL, width = 8, flag = 0)
tax$BAN_REAL <- as.character(tax$BAN_REAL)


#================================================================
#4. HS code輸入位置:使用grepl作篩選，故各hscode皆可輸入
#運算單一產業&子類
input_industry <- '機械'
all_codes <- read.xlsx(paste0('/','產業/2 HScode/確定版總表.xlsx'), 1,
                       encoding='UTF-8',stringsAsFactors = F) %>% filter(industry %in% input_industry)

#批次運算多產業
all_codes <- read.xlsx(paste0('/ds/','產業/2 HScode/確定版總表.xlsx'), 1,
                       encoding='UTF-8',stringsAsFactors = F) 

all_codes[all_codes$industry==all_codes$class,'class'] <- 'All'

country_map <- read_csv('/ds/出口市場篩選/產業別出口市場/ds_export_markets.csv',
                        col_types = c('ccc______'))

#================================================================
#5. 國家名稱
country_name <- country_map$export_countryName[-c(210:218)]
# 

#================================================================
#join all tables
#1. industry by country 
for (i in 1:nrow(all_codes)){
  class <- all_codes[i, 'class']
  codes <- all_codes[i, 'HS.Code']
  industry <- all_codes[i,'industry']
  
  #export table
  export_df <- process_export(codes)
  
  #import table
  import_df_tmp <- process_import(codes)
  import_df <- import_df_tmp[[1]]
  top_countries <- import_df_tmp[[2]]
  #company
  # company_df <- process_company(codes)
  
  rs <- full_join(export_df,import_df, by='國家') # %>% left_join(company_df, by='國家')
  rs$category <- class
  rs$industry <- industry
  
  rs$中小企業出口商數[is.na(rs$中小企業出口商數)] <- 0
  rs$出口商數[is.na(rs$出口商數)] <- 0
  rs$前三大出口商占比[is.na(rs$前三大出口商占比)] <- 0
  
  write.xlsx(rs, file = paste0('//172.20.23.190/ds/出口市場篩選/產品別出口目標市場/產業別選市場/data_vickey_machine/出口/',industry,'_',class,'.xlsx'), row.names = F)

  col.name <- names(rs)[grepl('國家|2014|2015|2016', names(rs))]
  col.name <- col.name[!grepl('排名|優於', col.name)]
  rs <- rs %>% select(col.name)
  if(i==1){
    write.xlsx(rs, file = '//172.20.23.190/ds/出口市場篩選/產品別出口目標市場/產業別選市場/data2017_10_02/mach_data_na1.xlsx',
               sheetName = class, row.names = F)
  
  }else{
    write.xlsx(rs, file = '//172.20.23.190/ds/出口市場篩選/產品別出口目標市場/產業別選市場/data2017_10_02/mach_data_na1.xlsx',
               sheetName = class, row.names = F, append = T)
  }
  
  
  print(paste0(i,'/',nrow(all_codes)))
}


rs[,c(2:5,7:9,11:14,16:17,19:21,23:26)] <- map(rs[,c(2:5,7:9,11:14,16:17,19:21,23:26)],as.numeric)
rs[,8][!is.finite(rs[,8])] <- '-'
rs[,9][!is.finite(rs[,9])] <- '-'
rs[,17][!is.finite(rs[,20])] <- '-'
rs[,18][!is.finite(rs[,21])] <- '-'
rs[is.na(rs)]<-'-'
rs[rs$`2016臺灣出口至全球總額`=='-','2016臺灣出口至全球總額']<- 0
rs[rs$`2016臺灣出口至他國總額`=='-','2016臺灣出口至他國總額']<- 0
rs[rs$`2016他國自全球進口總額`=='-','2016他國自全球進口總額']<- 0
rs[rs$`2016他國自臺灣進口總額`=='-','2016他國自臺灣進口總額']<- 0
rs_temp <- read_excel(paste0('//172.20.23.190/ds/出口市場篩選/產業別出口市場/data_all/',paste0(country_name[j],'.xlsx')))
rs <- rbind(rs_temp, rs) %>% data.frame()

write.xlsx(rs, file = paste0('//172.20.23.190/ds/出口市場篩選/產業別出口市場/data_all_add_industry/',paste0(country_name[j],'.xlsx')), row.names = F)
