#---- import packages ----
library(googleCloudStorageR)
library(bigrquery)
library(DBI)
library(ggplot2)
library(dplyr)
library(tidyr)
library(zoo)
library(lubridate)

#---- function to pull data from API
response_pull <- function(data_to_forecast, type = c('ticker', 'other'),
                          date_var_name = NULL, y_var_name = NULL, full_endpoint_input = NULL, 
                          start_date = '2019-01-01', end_date = Sys.Date()
){
  library(httr)
  library(jsonlite)
  
  if(type == 'ticker'){
    
    #May want to update to be able to define granularity as well
    
    api_endpoint <- paste0("freight/ticker/", data_to_forecast,"/USA/",start_date,"/", end_date)
    date_var <- 'data_Timestamp'
    y_var <- 'data_Value'
  }else{
    api_endpoint <- full_endpoint_input
    date_var <- date_var_name
    y_var <- y_var_name
  }
  
  u <- Sys.getenv("fw_api_un")
  pw <- Sys.getenv("fw_api_pw")
  post_body <- paste0("{'username': '",u ,"', 'password': '", pw,"'}")
  CredentialDataSet <- POST("https://api.freightwaves.com/Credential/authenticate",
                            body = post_body, 
                            encode = "raw",
                            content_type('application/json'))
  c1 <- content(CredentialDataSet, "text")
  c2 <- fromJSON(c1, flatten = TRUE)
  credential_df <- as.data.frame(c2)
  token <- as.character(credential_df[1,2])
  base_url <- "https://api.freightwaves.com/"
  call <- paste0(base_url, api_endpoint)
  key <- paste0("Bearer ", token)
  RawDataSet <- GET(call, add_headers(Authorization = key))
  Clean1 <- content(RawDataSet, "text")
  Clean2 <- fromJSON(Clean1, flatten = TRUE)
  response_df <- as.data.frame(Clean2) %>% 
    select(!!c(date_var, y_var)) %>%
    mutate(ds = as.Date(data_Timestamp)) %>% 
    rename(y = data_Value) %>% select(ds, y)
  
  return(response_df)
}

#---- load data ----
doe_FW_rd <- response_pull(data_to_forecast='DOE', type='ticker', start_date='2018-10-27')

bqcon <- dbConnect(
  bigrquery::bigquery(),
  project = "freightwaves-data-factory"
)

query_LOH_rd = 
  "with sums as (
      select
        data_timestamp + 1 as data_timestamp
        ,effective_miles
        ,round( safe_divide(effective_fuel_charge, effective_miles), 2 ) as fuel_rpm
      from `freightwaves-data-factory.output_trucking_rates.contract_output_history`
      where data_timestamp > '2018-10-27'
    ),
    LOH as (
      select
        data_timestamp
        ,case 
          when effective_miles < 100 then 'city'
          when effective_miles < 250 then 'short'
          when effective_miles < 500 then 'mid'
          when effective_miles < 750 then 'tweener'
          when effective_miles < 1000 then 'long'
          else 'extra-long'
          end as LOH_type
        ,fuel_rpm
      from sums
    ),
    median as (
      select
        data_timestamp, LOH_type
        ,percentile_cont(fuel_rpm, 0.5) over(partition by data_timestamp, LOH_type) as fuel_rpm
      from LOH
    ),
    final as (
      select
        data_timestamp, LOH_type
        ,max(fuel_rpm) as fuel_rpm
      from median
      group by data_timestamp, LOH_type
    )
    
    select * from final
    order by data_timestamp, LOH_type"

LOH_df_rd    <- dbGetQuery(bqcon, query_LOH_rd)

#---- create low, mid, and high projections ----
fc  <- list(low_fc=1,  mid_fc=1.5, high_fc=2)
mpg <- list(low_mpg=5, mid_mpg=6,  high_mpg=7)

doe_FW_ranges_rd <- doe_FW_rd %>% 
  mutate(doe_avg  = zoo::rollmean(y, 10, fill=NA, align='left'),
         low_fsc  = (doe_avg - fc$high_fc)/mpg$high_mpg,
         mid_fsc  = (doe_avg - fc$mid_fc) /mpg$mid_mpg,
         high_fsc = (doe_avg - fc$low_fc) /mpg$low_mpg,
         doe_rpm  = doe_avg/mpg$mid_mpg) %>% 
  tidyr::drop_na() %>% 
  tidyr::pivot_longer(cols=c('low_fsc', 'mid_fsc', 'high_fsc'), names_to='rate', values_to='value')

#---- combine data ----
combined_LOH_df_rd <- doe_FW_ranges_rd %>% 
  left_join(LOH_df_rd, by=c('ds'='data_timestamp')) %>% 
  mutate(LOH_type = factor(LOH_type, levels=c('city','short','mid','tweener','long','extra-long')))

#---- plot data ----
fuel_rate_plotter_rd <- function(data, rate_type) {
  
  plot <- data %>%
    ggplot(aes(ds, value, fill=rate)) +
    geom_line() +
    theme_classic() +
    labs(x='', y='Fuel RPM') +
    ggtitle(paste0(rate_type, ' Fuel Rates vs Projected Range (FW calculated rate data)'))
  
  return(plot)
  
}

LOH_plot_rd <- combined_LOH_df_rd %>% 
  fuel_rate_plotter_rd('LOH') +
  geom_line(aes(ds, fuel_rpm, color=LOH_type))
LOH_plot_rd
