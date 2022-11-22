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
doe_FW_rd <- response_pull(data_to_forecast='DOE', type='ticker', start_date='2018-10-27')

bqcon <- dbConnect(
  bigrquery::bigquery(),
  project = "freightwaves-data-factory"
)

query_main_rd = 
  "with sums as (
      select
        data_timestamp + 1 as data_timestamp
        ,mode as mode_type
        ,round( safe_divide(effective_fuel_charge, miles), 2 ) as fuel_rpm
      from `freightwaves-data-factory.output_trucking_rates.contract_output_history`
      where data_timestamp > '2018-10-27'
    ),
    median as (
      select
        data_timestamp, mode_type
        ,percentile_cont(fuel_rpm, 0.5) over(partition by data_timestamp, mode_type) as fuel_rpm
      from sums
    ),
    final as (
      select
        data_timestamp, mode_type
        ,max(fuel_rpm) as fuel_rpm
      from median
      group by data_timestamp, mode_type
    )
    
    select * from final
    order by data_timestamp, mode_type"

query_PADD_rd = 
  "with sums as (
      select
        data_timestamp + 1 as data_timestamp
        ,cast( substr(od_pair, 1, 3) as int64 ) as origin_zip3
        ,cast( substr(od_pair, 5, 3) as int64 ) as dest_zip3
        ,round( safe_divide(effective_fuel_charge, miles), 2 ) as fuel_rpm
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

query_region_rd = 
  "with sums as (
      select
        data_timestamp + 1 as data_timestamp
        ,substr(od_pair, 1, 1) as origin_zip3
        ,substr(od_pair, 5, 1) as dest_zip3
        ,round( safe_divide(effective_fuel_charge, miles), 2 ) as fuel_rpm
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

query_LOH_rd = 
  "with sums as (
      select
        data_timestamp + 1 as data_timestamp
        ,miles
        ,round( safe_divide(effective_fuel_charge, miles), 2 ) as fuel_rpm
      from `freightwaves-data-factory.output_trucking_rates.contract_output_history`
      where data_timestamp > '2018-10-27'
    ),
    LOH as (
      select
        data_timestamp
        ,case 
          when miles <  100 then 'city'
          when miles <  250 then 'short'
          when miles <  500 then 'mid'
          when miles <  750 then 'tweener'
          when miles < 1000 then 'long'
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

main_df_rd   <- dbGetQuery(bqcon, query_main_rd)
PADD_df_rd   <- dbGetQuery(bqcon, query_PADD_rd)
region_df_rd <- dbGetQuery(bqcon, query_region_rd)
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
combined_main_df_rd   <- doe_FW_ranges_rd %>% 
  left_join(main_df_rd,   by=c('ds'='data_timestamp')) %>%
  # add reference surcharge schedules
  mutate(todd_FSC = (y-1.2)/6,
         HEB_FSC = ceiling( ((y-1.22)/6) *100 )/100,
         HBC_FSC = ceiling( ((y-1.15)/5) *100 )/100,
         Genpak_FSC = ceiling( ((y-1.11)/6) *100 )/100)

combined_PADD_df_rd   <- doe_FW_ranges_rd %>% 
  left_join(PADD_df_rd,   by=c('ds'='data_timestamp'))

combined_region_df_rd <- doe_FW_ranges_rd %>% 
  left_join(region_df_rd, by=c('ds'='data_timestamp'))

combined_LOH_df_rd    <- doe_FW_ranges_rd %>% 
  left_join(LOH_df_rd,    by=c('ds'='data_timestamp')) %>% 
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

main_plot_rd <- combined_main_df_rd %>%
  fuel_rate_plotter_rd('Mode') +
  geom_line(aes(ds, fuel_rpm, color=mode_type))
main_plot_rd

PADD_plot_rd <- combined_PADD_df_rd %>% 
  fuel_rate_plotter_rd('PADD') +
  geom_line(aes(ds, fuel_rpm, color=PADD_type))
PADD_plot_rd

region_plot_rd <- combined_region_df_rd %>% 
  fuel_rate_plotter_rd('Region') +
  geom_line(aes(ds, fuel_rpm, color=lane_region))
region_plot_rd

LOH_plot_rd <- combined_LOH_df_rd %>% 
  fuel_rate_plotter_rd('LOH') +
  geom_line(aes(ds, fuel_rpm, color=LOH_type))
LOH_plot_rd

#---- boxplots ----
# Old code used to look at PADD rates broken out by mode_type.
# Implementing this requires a different query that includes mode_type.
# combined_PADD_df_rd %>% 
#   ggplot() +
#   geom_hline(yintercept=doe_fsc_ranges_avg) +
#   theme_classic() +
#   geom_boxplot(aes(mode_type, fuel_rpm)) +
#   facet_wrap('PADD_type') +
#   labs(x='', y='Fuel RPM', title='PADD Fuel Rates vs Projected Range (2019 - present, FW calculated rate data)')
