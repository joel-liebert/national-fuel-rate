#---- import packages ----
library(googleCloudStorageR)
library(bigrquery)
library(DBI)
library(ggplot2)
library(dplyr)
library(tidyr)
library(zoo)
library(lubridate)

source('./utils.R')

#---- load data ----
doe_FW <- response_pull(data_to_forecast='DOE', type='ticker', start_date='2018-10-27')

bqcon <- dbConnect(
  bigrquery::bigquery(),
  project = "freightwaves-data-factory"
)

query_main = 
  "with sums as (
      select
        data_timestamp + 1 as data_timestamp
        ,mode
        ,sum(effective_miles) as effective_miles_sum
        ,sum(effective_fuel_charge) as effective_fuel_charge_sum
      from `freightwaves-data-factory.output_trucking_rates.contract_output_history`
      where data_timestamp > '2018-10-27'
      group by data_timestamp, mode
    ),
    final as (
      select
        data_timestamp, mode
        ,round( safe_divide(effective_fuel_charge_sum, effective_miles_sum), 2 ) as fuel_rpm
      from sums
    )
    
    select * from final
    order by data_timestamp, mode"

query_PADD = 
  "with sums as (
      select
        data_timestamp + 1 as data_timestamp
        ,cast( substr(od_pair, 1, 3) as int64 ) as origin_zip3
        ,cast( substr(od_pair, 5, 3) as int64 ) as dest_zip3
        ,round( safe_divide(effective_fuel_charge, effective_miles), 2 ) as fuel_rpm
      from `freightwaves-data-factory.output_trucking_rates.contract_output_history`
      where data_timestamp > '2018-10-27'
    ),
    PADD as (
      select
        data_timestamp
        ,case 
          when origin_zip3  > 888 and dest_zip3  > 888 then 'full PADD5'
          when origin_zip3  > 888 or  dest_zip3  > 888 then 'half PADD5'
          when origin_zip3 <= 888 and dest_zip3 <= 888 then 'not PADD5'
          end as PADD_type
        ,fuel_rpm
      from sums
    ),
    median as (
      select
        data_timestamp, PADD_type
        ,percentile_cont(fuel_rpm, 0.5) over(partition by data_timestamp, PADD_type) as fuel_rpm
      from PADD
    ),
    final as (
      select
        data_timestamp, PADD_type
        ,max(fuel_rpm) as fuel_rpm
      from median
      group by data_timestamp, PADD_type
    )
    
    select * from final
    order by data_timestamp, PADD_type"

query_region = 
  "with sums as (
      select
        data_timestamp + 1 as data_timestamp
        ,substr(od_pair, 1, 1) as origin_zip3
        ,substr(od_pair, 5, 1) as dest_zip3
        ,round( safe_divide(effective_fuel_charge, effective_miles), 2 ) as fuel_rpm
      from `freightwaves-data-factory.output_trucking_rates.contract_output_history`
      where data_timestamp > '2018-10-27'
    ),
    regions as (
      select
        data_timestamp
        ,case 
          when origin_zip3 in ('0','1','2') then 'atlantic'
          when origin_zip3 in ('3','7')     then 'south'
          when origin_zip3 in ('4','5','6') then 'midwest'
          when origin_zip3 in ('8','9')     then 'west'
          end as orig_region
        ,case 
          when dest_zip3 in ('0','1','2') then 'atlantic'
          when dest_zip3 in ('3','7')     then 'south'
          when dest_zip3 in ('4','5','6') then 'midwest'
          when dest_zip3 in ('8','9')     then 'west'
          end as dest_region
        ,fuel_rpm
      from sums
    ),
    lane_regions as (
      select
        data_timestamp
        ,concat( greatest(orig_region, dest_region), '-', least(orig_region, dest_region) ) as lane_region
        ,fuel_rpm
      from regions
    ),
    median as (
      select
        data_timestamp, lane_region
        ,percentile_cont(fuel_rpm, 0.5) over(partition by data_timestamp, lane_region) as fuel_rpm
      from lane_regions
    ),
    final as (
      select
        data_timestamp, lane_region
        ,max(fuel_rpm) as fuel_rpm
      from median
      group by data_timestamp, lane_region
    )
    
    select * from final
    order by data_timestamp, lane_region"

query_LOH = 
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

main_df   <- dbGetQuery(bqcon, query_main)
PADD_df   <- dbGetQuery(bqcon, query_PADD)
region_df <- dbGetQuery(bqcon, query_region)
LOH_df    <- dbGetQuery(bqcon, query_LOH)

#---- create low, mid, and high projections ----
fc  <- list(low_fc=1,  mid_fc=1.5, high_fc=2)
mpg <- list(low_mpg=5, mid_mpg=6,  high_mpg=7)

doe_FW_ranges <- doe_FW %>% 
  mutate(doe_avg  = zoo::rollmean(y, 10, fill=NA, align='left'),
         low_fsc  = (doe_avg - fc$high_fc)/mpg$high_mpg,
         mid_fsc  = (doe_avg - fc$mid_fc) /mpg$mid_mpg,
         high_fsc = (doe_avg - fc$low_fc) /mpg$low_mpg,
         doe_rpm  = doe_avg/mpg$mid_mpg) %>% 
  tidyr::drop_na() %>% 
  tidyr::pivot_longer(cols=c('low_fsc', 'mid_fsc', 'high_fsc'), names_to='rate', values_to='value')

#---- combine data ----
combined_main_df   <- doe_FW_ranges %>% 
  left_join(main_df,   by=c('ds'='data_timestamp')) %>% 
  # right_join(todd_df,  by=c('ds'='DATE')) %>% 
  mutate(todd_FSC = (y-1.2)/6,
         HEB_FSC = ceiling( ((y-1.22)/6) *100 )/100,
         HBC_FSC = ceiling( ((y-1.15)/5) *100 )/100,
         Genpak_FSC = ceiling( ((y-1.11)/6) *100 )/100)

combined_PADD_df   <- doe_FW_ranges %>% 
  left_join(PADD_df,   by=c('ds'='data_timestamp'))

combined_region_df <- doe_FW_ranges %>% 
  left_join(region_df, by=c('ds'='data_timestamp'))

combined_LOH_df    <- doe_FW_ranges %>% 
  left_join(LOH_df,    by=c('ds'='data_timestamp'))
#---- plot data ----
fuel_rate_plotter <- function(data, rate_type) {

  plot <- data %>%
    ggplot(aes(ds, value, fill=rate)) +
    geom_line() +
    theme_classic() +
    labs(x='', y='Fuel RPM') +
    ggtitle(paste0(rate_type, ' Fuel Rates vs Projected Range (based on ten-week rolling average of DOE.USA)'))

  return(plot)

}

main_plot <- combined_main_df %>%
  fuel_rate_plotter('Mode') +
  geom_line(aes(ds, fuel_rpm, color=mode))
main_plot

PADD_plot <- combined_PADD_df %>% 
  fuel_rate_plotter('PADD') +
  geom_line(aes(ds, fuel_rpm, color=PADD_type))
PADD_plot

region_plot <- combined_region_df %>% 
  fuel_rate_plotter('Region') +
  geom_line(aes(ds, fuel_rpm, color=lane_region))
region_plot

LOH_plot <- combined_LOH_df %>% 
  fuel_rate_plotter('LOH') +
  geom_line(aes(ds, fuel_rpm, color=LOH_type))
LOH_plot
