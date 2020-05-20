library(readr)
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(readxl)
library(xlsx)


#依產業算進口
#Take a vector of HS codes and return a vector of statistics.
process_import <- function(codes) {
  
  # functions
  all_calc <- function(t1){
    
    for(j in 1:length(year_list)){
      v_col <- paste0('v',year_list[j])
      w_col <- paste0('wkg', year_list[j])
      
      if(j > 1){
        t1[paste0('growth_rate_', year_list[j])] <- round((t1[v_col]/t1[paste0('v',year_list[j]-1)] -1) *100, 2)
      }
      t1[paste0('price_', year_list[j])] <- round(t1[v_col]/t1[w_col], 2)
      t1[paste0('weight_', year_list[j])] <- round((t1[v_col]/sum(t1[v_col], na.rm = T)) *100, 2)
      t1[paste0('rank_', year_list[j])] <- rank(desc(t1[v_col]), ties.method = 'max')
      t1[paste0('total_import', year_list[j])] <- sum(t1[v_col], na.rm = T)
      t1[paste0('total_wkg', year_list[j])] <- sum(t1[w_col], na.rm = T)
      t1[paste0('total_price', year_list[j])] <- 
        round(t1[paste0('total_import', year_list[j])]/t1[paste0('total_wkg', year_list[j])], 2) 
      
      if(j == length(year_list)){
        t1[paste0('cagr3yr')] <- 
          round(((t1[[v_col]]/t1[[paste0('v',year_list[j]-2)]])^(1/2)-1) *100, 2)
        t1[paste0('cagr5yr')] <- 
          round(((t1[[v_col]]/t1[[paste0('v',year_list[j]-4)]])^(1/4)-1) *100, 2)
        
        t1[paste0('cagr_world3yr')] <- 
          round(((t1[[paste0('total_import', year_list[j])]]/t1[[paste0('total_import', year_list[j]-2)]])^(1/2) -1) *100, 2)
        t1[paste0('cagr_world5yr')] <- 
          round(((t1[[paste0('total_import', year_list[j])]]/t1[[paste0('total_import', year_list[j]-4)]])^(1/4) -1) *100, 2)
        
        t1[paste0('country_count_',year_list[j])] <- nrow(t1)
        t1[paste0('top3_', year_list[j])] <- paste0(t1$partner[1:3],'(',t1[[paste0('weight_', year_list[j])]][1:3],'%)', collapse = '; ')
      }
      
    }
    
    t1$rankup <- ifelse(t1[[paste0('rank_', year_list[j])]] < t1[[paste0('rank_', year_list[j]-1)]],'Y','N')
    t1$gup3yr <- ifelse(t1[[paste0('cagr3yr')]] > t1[[paste0('cagr_world3yr')]],'Y','N')
    t1$gup5yr <- ifelse(t1[[paste0('cagr5yr')]] > t1[[paste0('cagr_world5yr')]],'Y','N')
    
    t1[is.na(t1)] <- 0
    
    if(!'臺灣' %in% t1$partner){
      t1 <- rbind(t1, data.frame(reporter=t1$reporter[1],partner='臺灣',t1[1,3:55]))
      col.na <- names(t1)[!grepl('reporter|partner|total|world|country|top3', names(t1))]
      t1[nrow(t1), col.na] <- 0
    }
    
    return(t1)
  }
  
  tbl_top_country <- function(t1){
    t1 <- t1 %>% select(reporter,partner,v2015,v2016) %>% arrange(desc(v2016))
    t1$weight <- round((t1$v2016/sum(t1$v2016))*100,2)
    t1$growth <- round((t1$v2016/t1$v2015 -1)*100,2)
    
    top_importer <- function(rank_n){
      tmp <- t1[rank_n,c('reporter','partner','v2016','weight','growth')]
      names(tmp) <- c('國家',paste0(rank_n, '_', c('進口國','2016年進口額(美元)',
                                                 '該國進口占比(%)','2015/16年成長率(%)')))
      tmp
    }
    
    top1 <- top_importer(1)
    top2 <- top_importer(2)
    top3 <- top_importer(3)
    
    tbl_top <- left_join(top1, top2, '國家') %>% left_join(top3, '國家')
    tbl_top
  }
  
  
  #======資料篩選=====
  product_list <- codes %>% str_replace_all(' ','') %>% strsplit(',') %>% unlist() %>% substr(1,6) %>% unique()
  product_list <- paste0('^', product_list, collapse = '|')
  product_list <- unique(importData$hscode6)[grepl(product_list, unique(importData$hscode6))]
  
  #篩國家資料
  UNdata <- importData %>% filter(hscode6 %in% product_list)

  #calculate statistical data
  df <- aggregate(.~reporter+partner, UNdata[,-3], sum, na.rm=T, na.action = NULL)
  
  substrRight <- function(x){
    substr(x, nchar(x)-3, nchar(x))
  }
  
  year_list <- substrRight(names(df)[-(1:2)]) %>% unique() %>% as.numeric()
  
  
  # calculation
  t<- data.frame()
  t2 <- data.frame()
  for (i in 1:length(unique(df$reporter))){
    t1 <- filter(df, reporter== unique(df$reporter)[i]) %>% arrange(desc(v2016))
    t2 <- rbind(t2, tbl_top_country(t1))
    
    t1 <- all_calc(t1)
    t <- rbind(t,t1)
  }
  #全球
  world <- aggregate(.~partner, df[,-c(1)],sum) %>% arrange(desc(v2016))
  world$reporter <- '全球'
  
  #東協
  df_asean <- df[df$reporter%in%c("泰國","越南","印尼","馬來西亞","新加坡","菲律賓","緬甸","柬埔寨","汶萊","寮國"),]
  ASEAN <- aggregate(.~partner, df_asean[,-c(1)],sum) %>% arrange(desc(v2016))
  ASEAN$reporter <- '東協'
  
  t2 <- rbind(tbl_top_country(world), t2)
  world <- all_calc(world)
  ASEAN <- all_calc(ASEAN)
  
  t <- rbind(t, world, ASEAN) %>% filter(partner=='臺灣')
  
  t <- t[,c('reporter', paste0('v',year_list), paste0('wkg', year_list),paste0('price_', year_list),
            paste0('weight_', year_list),paste0('growth_rate_', year_list[-1]),
            paste0('total_import', year_list), paste0('total_wkg', year_list),
            paste0('total_price', year_list), paste0('rank_',year_list),  'rankup',
            'cagr3yr','cagr_world3yr','gup3yr','cagr5yr','cagr_world5yr','gup5yr',
            'country_count_2016','top3_2016')] %>% arrange(desc(v2016))
  
  names(t) <- c('國家', paste0('自臺進口額(美元)',year_list,'年'), paste0('自臺進口重量(kg)', year_list,'年'),
                paste0('自臺進口單價($/kg)', year_list,'年'), paste0('自臺進口比重(%)', year_list,'年'),
                paste0('自臺進口成長率(%)', year_list[-1],'年'),paste0('該國總進口額(美元)', year_list,'年'),
                paste0('該國總進口重量(kg)', year_list,'年'),paste0('該國總進口平均單價($/kg)', year_list,'年'),
                paste0('自臺進口在該國排名',year_list), '排名是否上升',
                paste0(year_list[3],'-',tail(year_list, 1),'年自臺進口複合成長率(%)'),
                paste0(year_list[3],'-',tail(year_list, 1),'年自全球進口複合成長率(%)'),
                paste0(year_list[3],'-',tail(year_list, 1),'年該國自臺CAGR是否優於全球'),
                paste0(year_list[1],'-',tail(year_list, 1),'年自臺進口複合成長率(%)'),
                paste0(year_list[1],'-',tail(year_list, 1),'年自全球進口複合成長率(%)'),
                paste0(year_list[1],'-',tail(year_list, 1),'年該國自臺CAGR是否優於全球'),
                '進口來源國數','該國前三大進口來源國')
  
  t2 <- t2 %>% filter(`1_2016年進口額(美元)` > 0)
  t <- list(t,t2)
  return(t)
}
