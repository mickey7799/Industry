library(readr)
#world <- read_csv('//172.20.23.190/ds/出口市場篩選/data/allCountryFromWorld12-15.csv',
                  #col_types = '__c______c__c________cc________d___')

rawData <- read.csv('//172.26.1.102/dstore/uncomtrade/annual_reduced/without-weight-kg/un-import-hs6-2012-2015.csv', 
                    colClasses = c(rep('numeric', 2),'character',rep('numeric',4)))
#rawData <- read.csv('//172.26.1.102/dstore/uncomtrade/annual_reduced/with-weight-kg/un-import-hs6-2012-2016.csv', 
#                    colClasses = c(rep('numeric', 2),'character', rep('NULL',3), 'numeric', 'NULL', 'numeric', 'NULL', 'numeric', 'NULL', 'numeric'))
                    


library(dplyr)
library(stringr)
library(tidyr)
library(stats)
library(XLConnect)
library(data.table)
source('//172.20.23.190/ds/Scripts/equalizr.r', encoding = 'UTF-8')

rawData$reporter_code <- str_pad(rawData$reporter_code, 3, pad = '0')
rawData$partner_code <- str_pad(rawData$partner_code, 3, pad = '0')
rawData$hscode6 <- str_pad(rawData$hscode6, 6, pad = '0')
rawData[is.na(rawData)] <- 0

world1 <- filter(rawData, partner_code=='000', reporter_code!='490')
tw1 <- filter(rawData, partner_code=='490')
world1 <- gather(world1, year, value, 4:7)
tw1 <- gather(tw1, year, value, 4:7)

df <- left_join(world1, tw1, by = c('year', 'reporter_code', 'hscode6'), suffix = c('.w', '.tw')) %>%
  select(year, reporter_code, hscode6, value.w, value.tw)
names(df) <- c('year', 'reporter_code', 'code', 'value.w', 'value.tw')
df[is.na(df$value.tw) == TRUE, 'value.tw'] <- 0

#mapping for reporter & partner
country_name <- read_csv(file = '//172.20.23.190/ds/Raw Data/2016大數爬蟲案/data/ITC HS6/itc_df_complete.csv',
                         col_types = c('cc_'))
df <- merge(x = df, y = country_name, by.x = 'reporter_code', by.y = 'ds_code')
colnames(df)[6] <- 'reporter'
df <- df[,c(2,6,3:5)]

names(df)<- c("year", "reporter", "code", "value.w", "value.tw") 


df$code <- factor(df$code, ordered = TRUE)
df$reporter <- equalize(df$reporter)

#replace character 'v'
df$year <- str_replace(df$year, 'v', '')

df.copy <- df

# Repair Dropout Data
dropouts <- data.frame()
for (ff in dir('C:/Users/2187/Documents/R/RCA/dropout/')) {
  obj.name <- str_replace(ff, '.txt', '') %>% str_replace(' ', '_')
  assign(obj.name, read_delim(paste0('C:/Users/2187/Documents/R/RCA/dropout/', ff),
                              delim = '\t',
                              col_types = 'cc_dd______dd____',
                              skip = 2,
                              col_names = FALSE))
  # df13 <- {
  #   gather(get(obj.name), key, value, -X1, -X2) %>%
  #     filter(key == 'X3' | key == 'X11') %>%
  #     spread(key, value) %>%
  #     mutate(year = '2013',
  #            reporter = str_replace(ff, '.txt', '')) %>%
  #     select(year, reporter, X1, X2, X11, X3) %>%
  #     filter(!is.na(X11))
  # }
  # names(df13) <- c('year', 'reporter', 'code', 'commodity', 'value.w', 'value.tw')
  df14 <- {
    gather(get(obj.name), key, value, -X1, -X2) %>%
      filter(key == 'X4' | key == 'X12') %>%
      spread(key, value) %>%
      mutate(year = '2014',
             reporter = str_replace(ff, '.txt', '')) %>%
      select(year, reporter, X1, X2, X12, X4) %>%
      filter(!is.na(X12))
  }
  names(df14) <- c('year', 'reporter', 'code', 'commodity', 'value.w', 'value.tw')
  df15 <- {
    gather(get(obj.name), key, value, -X1, -X2) %>%
      filter(key == 'X5' | key == 'X13') %>%
      spread(key, value) %>%
      mutate(year = '2015',
             reporter = str_replace(ff, '.txt', '')) %>%
      select(year, reporter, X1, X2, X13, X5) %>%
      filter(!is.na(X13))
  }
  names(df15) <- c('year', 'reporter', 'code', 'commodity', 'value.w', 'value.tw')
  dropouts <- rbind(dropouts, df14) %>% rbind(df15)
  # %>% rbind(df13)
}
# CAUTION!! Import value from ITC are in thousands
dropouts$value.w <- dropouts$value.w * 1000
dropouts$value.tw <- dropouts$value.tw * 1000
dropouts[is.na(dropouts$value.tw) == TRUE, 'value.tw'] <- 0


#check avalibility
df[df$reporter%in% c('Bangladesh', 'Myanmar', 'Cambodia', 
                     'United Arab Emirates', 'Viet Nam', 'Brunei', 'Lao', 'South Korea', 'Turkey', 'Indonesia')&df$year
   %in%c(2016,2015),] %>% group_by(reporter, year)%>% 
  summarise(sum(value.w),sum(value.tw))%>%View()
# No data for TW 2014 & 2015, remove:
# Bangladesh, Myanmar

# Only data for TW 2014 (already contained in df), remove:
# Cambodia, United Arab Emirates, Viet Nam
dropouts <- filter(dropouts,
                   !reporter %in% c('Bangladesh', 'Myanmar', 'Cambodia', 
                                    'United Arab Emirates', 'Viet Nam'))

# Complete data for TW 2014 & 2015, take:
# Brunei, Lao, South Korea, Turkey

# Complete data for TW 2014 & 2015, but only need 2015:
# Indonesia
dropouts <- filter(dropouts, !(reporter == 'Indonesia' & year == '2014'))

# Equalize
dropouts$reporter <- equalize(dropouts$reporter)

# Because df already contains 2014 data for Cambodia, Indonesia, United Arab Emirates,
# and Viet Nam, we will remove these rows from dropouts
# remove <- equalize(c('Cambodia', 'Indonesia', 'United Arab Emirates', 'Viet Nam'))
# dropouts$reporter <- equalize(dropouts$reporter)
# dropouts <- filter(dropouts, !(reporter %in% remove & year == '2014'))

#leave out column commodity
dropouts <- dropouts[,c(1:3,5:6)]

df <- rbind(df, dropouts)

# Compute Herfindahl index
herf <- {
  filter(df, year == '2015', reporter != 'EU-28') %>%
    group_by(reporter) %>%
    select(reporter, code, value.tw) %>%
    mutate(c.sum = sum(value.tw),
           share.sq = (value.tw / c.sum) ^ 2,
           n = n()) %>%
    group_by(reporter, n) %>%
    summarise(h = sum(share.sq)) %>%
    mutate(herf = (h - 1/n) / (1 - 1/n)) %>%
    select(reporter, herf)
}

# Output NRCA for HS6 products
outputHS <- function() {
  path_code <- '//172.20.23.190/ds/Raw Data/HS code分類表/2017/note_8_C.txt'
  ch.code <- fread(path_code, encoding = 'UTF-8', header = TRUE,
                   colClasses = c('character', 'character'))
  names(ch.code) <- c('code', 'ch.description')
  aa <- ungroup(df) %>%
    group_by(year, reporter) %>%
    mutate(sum.w = sum(value.w),
           sum.tw = sum(value.tw),
           perc.w = value.w / sum.w,
           perc.tw = value.tw / sum.tw,
           # Remove NaN's
           perc.tw = ifelse(is.nan(perc.tw), 0, perc.tw),
           rca = perc.tw / perc.w,
           # Remove NaN's
           rca = ifelse(is.nan(rca), 0, rca),
           nrca = (rca - 1) / (rca + 1)) %>%
    filter(year == '2015') %>%
    left_join(ch.code, by = 'code') %>%
    select(year, reporter, code, ch.description, commodity, nrca) %>%
    spread(reporter, nrca, fill = -1)
  aa <- aa[complete.cases(aa), ]
  write.table(aa, 'output.csv', sep = ',', quote = TRUE)
}


makeRegex <- function(x) {
  paste("", paste(x, collapse = "|^"), sep = "^")
}


# Plot Head Map by Sector
ctgy <- df
categorize <- function(df) {
  neg.list <- c("841451", "841510", "84189110", "84189910", "84212110", "842211",
                "842310", "8450", "845121", "845210", "8471", "847330",
                as.character(841821:841829)) %>% substr(1, 6)
  df$category <- NA
  df[str_pad(1:5, 2, 'left', '0') %>% makeRegex() %>% grepl(df$code), 'category'] <- "活動物；動物產品"
  df[str_pad(6:14, 2, 'left', '0') %>% makeRegex() %>% grepl(df$code), 'category'] <- "植物產品"
  df["15" %>% makeRegex() %>% grepl(df$code), 'category'] <- "動植物油脂"
  df[as.character(16:24) %>% makeRegex() %>% grepl(df$code), 'category'] <- "調製食品；飲料及菸酒"
  df[as.character(25:27) %>% makeRegex() %>% grepl(df$code), 'category'] <- "礦產品"
  df[as.character(28:38) %>% makeRegex() %>% grepl(df$code), 'category'] <- "化學品"
  df[as.character(39:40) %>% makeRegex() %>% grepl(df$code), 'category'] <- "塑膠、橡膠及其製品"
  df[as.character(41:43) %>% makeRegex() %>% grepl(df$code), 'category'] <- "毛皮及其製品"
  df[as.character(44:46) %>% makeRegex() %>% grepl(df$code), 'category'] <- "木及木製品"
  df[as.character(47:49) %>% makeRegex() %>% grepl(df$code), 'category'] <- "紙漿；紙及其製品；印刷品"
  df[as.character(50:63) %>% makeRegex() %>% grepl(df$code), 'category'] <- "紡織品"
  df[as.character(64:67) %>% makeRegex() %>% grepl(df$code), 'category'] <- "鞋、帽及其他飾品"
  df[as.character(68:70) %>% makeRegex() %>% grepl(df$code), 'category'] <- "非金屬礦物製品"
  df["71" %>% makeRegex() %>% grepl(df$code), 'category'] <- "珠寶及貴金屬製品"
  df[as.character(72:83) %>% makeRegex() %>% grepl(df$code), 'category'] <- "基本金屬及其製品"
  df[sapply(c(8532:8534, 8540:8542), as.character) %>% makeRegex() %>% grepl(df$code), 'category'] <- "電子零組件"
  df[grepl("^84", df$code) & !grepl(makeRegex(neg.list), df$code), 'category'] <- "機械"
  df[sapply(c(8501:8507, 8511:8512, 8514:8515, 8530:8531, 8535:8538, 8543:8548), as.character) %>%
       makeRegex() %>% grepl(df$code), 'category'] <- "電機產品"
  df[c("8471", "847330", as.character(8517:8529)) %>% makeRegex() %>% grepl(df$code), 'category'] <- "資通與視聽產品"
  df[c("841451", "841510", "84189110", "84189910", "84212110",
       "842211", "842310", "8450", "845121", "845210", "8513",
       "8516", "8539", sapply(c(841821:841829, 8508:8510), as.character)) %>% substr(1, 6) %>%
       makeRegex() %>% grepl(df$code), 'category'] <- "家用電器"
  df[as.character(86:89) %>% makeRegex() %>% grepl(df$code), 'category'] <- "運輸工具"
  df[as.character(90:92) %>% makeRegex() %>% grepl(df$code), 'category'] <- "光學及精密儀器；鐘錶；樂器"
  df[as.character(93:98) %>% makeRegex() %>% grepl(df$code), 'category'] <- "OTHERS"
  df["99" %>% makeRegex() %>% grepl(df$code), 'category'] <- "COMMODITIES NOT SPECIFIED"
  return(df)
}

ctgy2 <- {
  categorize(ctgy) %>%
    ungroup() %>%
    group_by(year, reporter, category) %>%
    summarise(ct.sum.w = sum(value.w),
              ct.sum.tw = sum(value.tw)) %>%
    mutate(sum.w = sum(ct.sum.w),
           sum.tw = sum(ct.sum.tw),
           perc.w = ct.sum.w / sum.w,
           perc.tw = ct.sum.tw / sum.tw,
           # Remove NaN's
           perc.tw = ifelse(is.nan(perc.tw), 0, perc.tw),
           rca = perc.tw / perc.w,
           # Remove NaN's
           rca = ifelse(is.nan(rca), 0, rca),
           nrca = (rca - 1) / (rca + 1),
           share= (ct.sum.tw / ct.sum.w) *100)
}
ctgy2 <- ctgy2[complete.cases(ctgy2), ]
any(is.na(ctgy2))


ctgy2$category <- factor(ctgy2$category, ordered = TRUE,
                         levels = c("活動物；動物產品", "植物產品", "動植物油脂", 
                                    "調製食品；飲料及菸酒", "礦產品", "化學品", 
                                    "塑膠、橡膠及其製品", "毛皮及其製品", "木及木製品", 
                                    "紙漿；紙及其製品；印刷品", "紡織品", 
                                    "鞋、帽及其他飾品", "非金屬礦物製品", "珠寶及貴金屬製品", 
                                    "基本金屬及其製品", "電子零組件", "機械", "電機產品", 
                                    "資通與視聽產品", "家用電器", "運輸工具", "光學及精密儀器；鐘錶；樂器", "OTHERS", "COMMODITIES NOT SPECIFIED"))

# find industry which share_growth >0
ctgy.raw <- {
  group_by(ctgy2, reporter, category) %>%
    mutate(share_growth = share - lag(share))
}

#to make sure each country has data (if no data in 2015, then use data in 2014)
cc13 <- unique(ctgy.raw[ctgy.raw$year == '2013', ]$reporter)
cc14 <- unique(ctgy.raw[ctgy.raw$year == '2014', ]$reporter)
cc15 <- unique(ctgy.raw[ctgy.raw$year == '2015', ]$reporter)
not.in.15 <- setdiff(cc14, cc15)
not.in.1514 <- setdiff(cc13, union(cc14, cc15))

ctgy.s <- {
  rbind(filter(ctgy.raw, year == '2015'),
        filter(ctgy.raw, year == '2014', reporter %in% not.in.15)) %>%
    rbind(filter(ctgy.raw, year == '2013', reporter %in% not.in.1514)) %>%
    select(reporter, category, nrca, share_growth) %>%
    arrange(reporter, desc(nrca), desc(share_growth))
}
ctgy.s <- ctgy.s[complete.cases(ctgy.s), ]

ct.perc <- {
  group_by(ctgy2, category) %>%
    summarise(par.sum = sum(ct.sum.tw)) %>%
    mutate(sum = sum(par.sum),
           percentage = par.sum / sum) %>%
    select(category, percentage)
}

outputWs <- function() {
  predata <- ungroup(ctgy.s) %>%
    mutate(share_max = max(share_growth),
           share_growth = share_growth / share_max,
           score = 0.01 * nrca + share_growth)
  wb <- loadWorkbook('C:/Users/2187/Desktop/大數據/數據中心業務/重點市場篩選指標/2018市場篩選/output.xls', create = TRUE)
  result <- NULL
  for (sctr in unique(predata$category)) {
    weight <- ct.perc[ct.perc$category == sctr, ]$percentage
    selected <- {
      predata %>%
        filter(category == sctr) %>%
        arrange(desc(score)) %>%
        top_n(15) %>%
        mutate(gp.score = 16 - row_number(),
               weighted.score = gp.score * weight)
    }
    result <- rbind(result, selected)
    createSheet(wb, sctr)
    writeWorksheet(wb, selected, sctr)
  }
  outcome <- {
    group_by(result, reporter) %>%
      summarise(ttl.score = sum(weighted.score)) %>%
      arrange(desc(ttl.score))
  }
  createSheet(wb, 'outcome')
  writeWorksheet(wb, outcome, 'outcome')
  saveWorkbook(wb, 'output.xls')
}
