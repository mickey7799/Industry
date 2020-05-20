library(readr)
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(readxl)
library(xlsx)
# Take a vector of HS codes & country and return a vector of statistics.
#codes = (character)hscodes seperated by , (eg. 5102,5320)
#country = (character)two letter country name (rg. IN)
process_export <- function(codes) {
  
  year_list <- start_yr:end_yr
  #===計算欄位用===#
  calcExport <- function(df){
    
    for(i in 1:length(year_list)){
      v_col <- paste0('t_value_',year_list[i])
      w_col <- paste0('w_value_', year_list[i])
      
      # export weight
      df[paste0('ex_weight_', year_list[i])] <- round(t1[v_col]/t1[[v_col]][1] *100, 2)
      # export price
      df[paste0('ex_price_', year_list[i])] <- round(t1[v_col]/t1[w_col], 2)
      
      if(i>1){
        # export growth
        df[paste0('ex_growth_', year_list[i])] <- 
          round((t1[v_col]/t1[paste0('t_value_',year_list[i-1])] -1) *100, 2)
      }
    }
    # CAGR 
    df['cagr3yr'] <- round(((t1[[v_col]]/t1[[paste0('t_value_', year_list[3])]])^(1/2)-1) *100, 2)
    df['gup3yr'] <- ifelse(df[['cagr3yr']] > df[['cagr3yr']][1],'Y','N')
    
    df['cagr5yr'] <- round(((t1[[v_col]]/t1[[paste0('t_value_', year_list[1])]])^(1/4)-1) *100, 2)
    df['gup5yr'] <- ifelse(df[['cagr5yr']] > df[['cagr5yr']][1],'Y','N')

    
    # df[,c(16:19,21:23)]<- do.call(data.frame,
    #                               lapply(df[,c(16:19,21:23)], function(x) replace(x, !is.finite(x),NA)))
    
    df <- df[,c('country', paste0('t_value_', year_list), paste0('ex_price_', year_list),
                paste0('ex_growth_', year_list[-1]), paste0('ex_weight_', year_list), 
                'cagr3yr','gup3yr','cagr5yr','gup5yr','rank2015','rank2016','rankup')]
    
    names(df) <- c('國家', paste0('臺灣出口額',year_list,'年'), paste0('臺灣出口單價',year_list,'($/kg)'),
                   paste0('臺灣出口成長率(%)', year_list[-1],'年'), paste0('臺灣出口比重(%)', year_list,'年'),
                   paste0('出口複合成長率(%)',year_list[3],'_',year_list[5],'年'), '出口他國複合成長率是否優於全球(3yr)',
                   paste0('出口複合成長率(%)',year_list[1],'_',year_list[5],'年'), '出口他國複合成長率是否優於全球(5yr)',
                   paste0('排名',year_list[4],'年'), paste0('排名', year_list[5],'年'), '是否上升')
    
    return(df)
  }
  
  
  product_list <- codes %>% str_replace_all(' ','') %>% strsplit(',') %>% unlist() %>% unique() %>% substr(1,10)
  product_list <- paste0('^', product_list, collapse = '|')
  product_list <- unique(custom$code)[grepl(product_list, unique(custom$code))]
  
  # #filter industry
  ind_custom <- filter(custom, code %in% product_list)
  ind_custom$value <- round(ind_custom$value *1000, 0) # 千美元 -> 美元
  
  if (nrow(ind_custom)==0){export_vec <- c(rep('0',2),rep('-',7))}else {
  
    # calculate statistical data
    t1 <- ind_custom %>% group_by(country, year) %>% summarise(t_value=sum(value), w_value=sum(weight))
    t1 <- dcast(setDT(t1),  country~year, value.var = c('t_value','w_value')) %>% as.data.frame()
    t1[is.na(t1)] <- 0
    
  
    all <- t1[!grepl('其他',t1$country, fixed = TRUE),]
    all <- arrange(all, desc(all[[paste0('t_value_', end_yr)]]))
    all$rank2015 <- ifelse(all[[paste0('t_value_', end_yr-1)]]!=0, rank(desc(all[[paste0('t_value_', end_yr-1)]])), NA)
    all$rank2016 <- ifelse(all[[paste0('t_value_', end_yr)]]!=0, rank(desc(all[[paste0('t_value_', end_yr)]])), NA)
    
    other <- t1[grepl('其他',t1$country, fixed = TRUE),]
    other <- arrange(other, desc(other[[paste0('t_value_', end_yr)]]))
    if(nrow(other) >0){other[,paste0('rank',2015:2016)] <- NA}
    
    t1 <- rbind(all, other)
    t1$rankup <- ifelse(t1$rank2016<t1$rank2015,'Y','N')
    
    newrow <- list('全球',
                   sum(t1[[2]], na.rm = T), sum(t1[[3]], na.rm = T), sum(t1[[4]], na.rm = T),
                   sum(t1[[5]], na.rm = T), sum(t1[[6]], na.rm = T), sum(t1[[7]], na.rm = T),
                   sum(t1[[8]], na.rm = T), sum(t1[[9]], na.rm = T), sum(t1[[10]], na.rm = T),
                   sum(t1[[11]], na.rm = T), '-', '-', '-')
    t1 <- rbind(newrow, t1)
    
    t1 <- calcExport(t1)
  }
  
  return(t1)
}
