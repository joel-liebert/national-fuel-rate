#---- import packages ----
library(googleCloudStorageR)
library(bigrquery)
library(DBI)
library(ggplot2)
library(dplyr)
library(tidyr)
library(zoo)
library(lubridate)
library(conflicted)

source('./utils.R')

#---- load data ----
doe <- response_pull(data_to_forecast='DOE', type='ticker', start_date='2018-10-27', end_date = '2022-11-14')

bqcon <- dbConnect(
  bigrquery::bigquery(),
  project = "freightwaves-data-factory"
)

query_mode = 
  "with filtered as (
      select
        ship_date as date
        ,date_diff(ship_date, '2018-10-27', week) as week
        ,transportation_mode_description as mode_type
        ,cast(origin_zip3 as int64) as origin_zip3
        ,cast(dest_zip3   as int64) as dest_zip3
        ,substr(origin_zip3, 1, 1) as origin_zip1
        ,substr(dest_zip3,   1, 1) as dest_zip1
        ,distance_imp as effective_miles
        ,fuel_charge_amount as effective_fuel_charge
        ,safe_divide(fuel_charge_amount, distance_imp) as fuel_rpm
      from `freightwaves-data-factory.data_enrichment.beetlejuice`
      where ship_date > '2018-10-27' and ship_date <= '2022-11-14'
        and transportation_mode_description in ('TRUCKLOAD (DRY VAN)', 'TRUCKLOAD (TEMP-CONTROLLED)')
        and distance_imp is not null and distance_imp > 0 
        and fuel_charge_amount is not null and fuel_charge_amount > 0
      order by ship_date
    ),
    ranks as (
      select
        *
        ,max(date) over (partition by week) + 2 as data_timestamp
        ,percent_rank() over (order by fuel_rpm) as rpm_rank
      from filtered
      where fuel_rpm is not null
    ),
    trimmed as (
      select * from ranks
      where rpm_rank > 0.05 and rpm_rank < 0.95
    ),
    mode as (
      select
        data_timestamp, mode_type
        ,percentile_cont(fuel_rpm, 0.5) over (partition by data_timestamp, mode_type) as fuel_rpm
      from trimmed
    ),
    mode_final as (
      select
        data_timestamp, mode_type
        ,max(fuel_rpm) as fuel_rpm
      from mode
      group by data_timestamp, mode_type
    )
    
    select * from mode_final
    order by data_timestamp, mode_type"

query_PADD = 
  "with filtered as (
      select
        ship_date as date
        ,date_diff(ship_date, '2018-10-27', week) as week
        ,transportation_mode_description as mode_type
        ,cast(origin_zip3 as int64) as origin_zip3
        ,cast(dest_zip3   as int64) as dest_zip3
        ,substr(origin_zip3, 1, 1) as origin_zip1
        ,substr(dest_zip3,   1, 1) as dest_zip1
        ,distance_imp as effective_miles
        ,fuel_charge_amount as effective_fuel_charge
        ,safe_divide(fuel_charge_amount, distance_imp) as fuel_rpm
      from `freightwaves-data-factory.data_enrichment.beetlejuice`
      where ship_date > '2018-10-27' and ship_date <= '2022-11-14'
        and transportation_mode_description in ('TRUCKLOAD (DRY VAN)', 'TRUCKLOAD (TEMP-CONTROLLED)')
        and distance_imp is not null and distance_imp > 0 
        and fuel_charge_amount is not null and fuel_charge_amount > 0
      order by ship_date
    ),
    ranks as (
      select
        *
        ,max(date) over (partition by week) +2 as data_timestamp
        ,percent_rank() over (order by fuel_rpm) as rpm_rank
      from filtered
      where fuel_rpm is not null
    ),
    trimmed as (
      select * from ranks
      where rpm_rank > 0.05 and rpm_rank < 0.95
    ),
    PADD as (
      select
        data_timestamp
        -- ,mode_type
        ,case 
          when origin_zip3  > 888 and dest_zip3  > 888 then 'full PADD5'
          when origin_zip3  > 888 or  dest_zip3  > 888 then 'half PADD5'
          when origin_zip3 <= 888 and dest_zip3 <= 888 then 'not PADD5'
          end as PADD_type
        ,fuel_rpm
      from trimmed 
    ),
    PADD_median as (
      select
        data_timestamp
        -- ,mode_type
        ,PADD_type
        ,percentile_cont(fuel_rpm, 0.5) over (partition by data_timestamp, PADD_type) as fuel_rpm
        -- ,percentile_cont(fuel_rpm, 0.5) over (partition by data_timestamp, mode_type, PADD_type) as fuel_rpm
      from PADD
    ),
    PADD_final as (
      select
        data_timestamp
        -- ,mode_type
        ,PADD_type
        ,max(fuel_rpm) as fuel_rpm
      from PADD_median
      group by data_timestamp, PADD_type
      -- group by data_timestamp, mode_type, PADD_type
    )
    
    select * from PADD_final
    order by data_timestamp, PADD_type
    -- order by data_timestamp, PADD_type, mode_type"

query_region = 
  "with filtered as (
      select
        ship_date as date
        ,date_diff(ship_date, '2018-10-27', week) as week
        ,transportation_mode_description as mode_type
        ,cast(origin_zip3 as int64) as origin_zip3
        ,cast(dest_zip3   as int64) as dest_zip3
        ,substr(origin_zip3, 1, 1) as origin_zip1
        ,substr(dest_zip3,   1, 1) as dest_zip1
        ,distance_imp as effective_miles
        ,fuel_charge_amount as effective_fuel_charge
        ,safe_divide(fuel_charge_amount, distance_imp) as fuel_rpm
      from `freightwaves-data-factory.data_enrichment.beetlejuice`
      where ship_date > '2018-10-27' and ship_date <= '2022-11-14'
        and transportation_mode_description in ('TRUCKLOAD (DRY VAN)', 'TRUCKLOAD (TEMP-CONTROLLED)')
        and distance_imp is not null and distance_imp > 0 
        and fuel_charge_amount is not null and fuel_charge_amount > 0
      order by ship_date
    ),
    ranks as (
      select
        *
        ,max(date) over (partition by week) +2 as data_timestamp
        ,percent_rank() over (order by fuel_rpm) as rpm_rank
      from filtered
      where fuel_rpm is not null
    ),
    trimmed as (
      select * from ranks
      where rpm_rank > 0.05 and rpm_rank < 0.95
    ),
    regions as (
      select
        data_timestamp
        -- ,mode_type
        ,case 
          when origin_zip1 in ('0','1','2') then 'atlantic'
          when origin_zip1 in ('3','7')     then 'south'
          when origin_zip1 in ('4','5','6') then 'midwest'
          when origin_zip1 in ('8','9')     then 'west'
          end as orig_region
        ,case 
          when dest_zip1 in ('0','1','2') then 'atlantic'
          when dest_zip1 in ('3','7')     then 'south'
          when dest_zip1 in ('4','5','6') then 'midwest'
          when dest_zip1 in ('8','9')     then 'west'
          end as dest_region
        ,fuel_rpm
      from trimmed
    ),
    lane_regions as (
      select
        data_timestamp
        -- ,mode_type
        ,concat( greatest(orig_region, dest_region), '-', least(orig_region, dest_region) ) as lane_region
        ,fuel_rpm
      from regions
    ),
    regions_median as (
      select
        data_timestamp
        -- ,mode_type
        ,lane_region
        ,percentile_cont(fuel_rpm, 0.5) over (partition by data_timestamp, lane_region) as fuel_rpm
        -- ,percentile_cont(fuel_rpm, 0.5) over (partition by data_timestamp, mode_type, lane_region) as fuel_rpm
      from lane_regions
    ),
    regions_final as (
      select
        data_timestamp
        -- ,mode_type
        ,lane_region
        ,max(fuel_rpm) as fuel_rpm
      from regions_median
      group by data_timestamp, lane_region
      -- group by data_timestamp, mode_type, lane_region
    )
    
    select * from regions_final
    order by data_timestamp, lane_region
    -- order by data_timestamp, lane_region, mode_type"

query_LOH = 
  "with filtered as (
      select
        ship_date as date
        ,date_diff(ship_date, '2018-10-27', week) as week
        ,transportation_mode_description as mode_type
        ,cast(origin_zip3 as int64) as origin_zip3
        ,cast(dest_zip3   as int64) as dest_zip3
        ,substr(origin_zip3, 1, 1) as origin_zip1
        ,substr(dest_zip3,   1, 1) as dest_zip1
        ,distance_imp as effective_miles
        ,fuel_charge_amount as effective_fuel_charge
        ,safe_divide(fuel_charge_amount, distance_imp) as fuel_rpm
      from `freightwaves-data-factory.data_enrichment.beetlejuice`
      where ship_date > '2018-10-27' and ship_date <= '2022-11-14'
        and transportation_mode_description in ('TRUCKLOAD (DRY VAN)', 'TRUCKLOAD (TEMP-CONTROLLED)')
        and distance_imp is not null and distance_imp > 0 
        and fuel_charge_amount is not null and fuel_charge_amount > 0
      -- group by data_timestamp, mode, origin_zip3, dest_zip3, origin_zip1, dest_zip1
      order by ship_date
    ),
    ranks as (
      select
        *
        ,max(date) over (partition by week) +2 as data_timestamp
        ,percent_rank() over (order by fuel_rpm) as rpm_rank
      from filtered
      where fuel_rpm is not null
    ),
    trimmed as (
      select * from ranks
      where rpm_rank > 0.05 and rpm_rank < 0.95
    ),
    LOH as (
      select
        data_timestamp
        -- ,mode_type 
        ,case 
          when effective_miles < 100 then 'city'
          when effective_miles < 250 then 'short'
          when effective_miles < 500 then 'mid'
          when effective_miles < 750 then 'tweener'
          when effective_miles < 1000 then 'long'
          else 'extra-long'
          end as LOH_type
        ,fuel_rpm
      from trimmed
    ),
    LOH_median as (
      select
        data_timestamp
        -- ,mode_type
        ,LOH_type
        ,percentile_cont(fuel_rpm, 0.5) over (partition by data_timestamp, LOH_type) as fuel_rpm
        -- ,percentile_cont(fuel_rpm, 0.5) over (partition by data_timestamp, mode_type, LOH_type) as fuel_rpm
      from LOH
    ),
    LOH_final as (
      select
        data_timestamp
        -- ,mode_type
        ,LOH_type
        ,max(fuel_rpm) as fuel_rpm
      from LOH_median
      group by data_timestamp, LOH_type
      -- group by data_timestamp, mode_type, LOH_type
    )
    
    select * from LOH_final
    order by data_timestamp, LOH_type
    -- order by data_timestamp, LOH_type, mode_type"

mode_df   <- dbGetQuery(bqcon, query_mode)
PADD_df   <- dbGetQuery(bqcon, query_PADD)
region_df <- dbGetQuery(bqcon, query_region)
LOH_df    <- dbGetQuery(bqcon, query_LOH)

#---- create low, mid, and high projections ----
fc  <- list(low_fc=1,  mid_fc=1.5, high_fc=2)
mpg <- list(low_mpg=5, mid_mpg=6,  high_mpg=7)

doe_fsc_ranges <- doe %>% 
  mutate(doe_avg  = zoo::rollmean(y, 10, fill=NA, align='left'),
         low_fsc  = (doe_avg - fc$high_fc)/mpg$high_mpg,
         mid_fsc  = (doe_avg - fc$mid_fc) /mpg$mid_mpg,
         high_fsc = (doe_avg - fc$low_fc) /mpg$low_mpg,
         doe_rpm  = doe_avg/mpg$mid_mpg) %>% 
  tidyr::drop_na() %>% 
  tidyr::pivot_longer(cols=c('low_fsc', 'mid_fsc', 'high_fsc'), names_to='rate', values_to='value')

doe_fsc_ranges_avg <- c(mean(doe_fsc_ranges$value[doe_fsc_ranges$rate=='low_fsc']),
                        mean(doe_fsc_ranges$value[doe_fsc_ranges$rate=='mid_fsc']),
                        mean(doe_fsc_ranges$value[doe_fsc_ranges$rate=='high_fsc']))

#---- combine data ----
combined_mode_df   <- doe_fsc_ranges %>% 
  left_join(mode_df, by=c('ds'='data_timestamp')) %>%
  mutate(mode_type = case_when(mode_type=='TRUCKLOAD (DRY VAN)'         ~ 'VAN',
                               mode_type=='TRUCKLOAD (TEMP-CONTROLLED)' ~ 'REEFER')) %>% 
  # add reference surcharge schedules
  mutate(todd_FSC   = (y-1.2)/6,
         HEB_FSC    = ceiling( ((y-1.22)/6) *100 )/100,
         HBC_FSC    = ceiling( ((y-1.15)/5) *100 )/100,
         Genpak_FSC = ceiling( ((y-1.11)/6) *100 )/100)

combined_PADD_df   <- doe_fsc_ranges %>% 
  left_join(PADD_df,   by=c('ds'='data_timestamp'))
# %>%
#   mutate(mode_type = case_when(mode_type=='TRUCKLOAD (DRY VAN)'         ~ 'VAN',
#                                mode_type=='TRUCKLOAD (TEMP-CONTROLLED)' ~ 'REEFER'))

combined_region_df <- doe_fsc_ranges %>% 
  left_join(region_df, by=c('ds'='data_timestamp'))
# %>%
#   mutate(mode_type = case_when(mode_type=='TRUCKLOAD (DRY VAN)'         ~ 'VAN',
#                                mode_type=='TRUCKLOAD (TEMP-CONTROLLED)' ~ 'REEFER'))

combined_LOH_df    <- doe_fsc_ranges %>% 
  left_join(LOH_df,    by=c('ds'='data_timestamp')) %>% 
  mutate(LOH_type = factor(LOH_type, levels=c('city','short','mid','tweener','long','extra-long')))
# %>%
#   mutate(mode_type = case_when(mode_type=='TRUCKLOAD (DRY VAN)'         ~ 'VAN',
#                                mode_type=='TRUCKLOAD (TEMP-CONTROLLED)' ~ 'REEFER'))

#---- plot data ----
plotter_template <- function(data, rate_type) {
  plot <- data %>%
    ggplot(aes(ds, value, fill=rate)) +
    geom_line() +
    theme_classic() +
    labs(x='', y='Fuel RPM') +
    ggtitle(paste0(rate_type, ' Fuel Rates vs Projected Range (enriched beetlejuice load data)'))

  return(plot)
}

boxplotter_template <- function(data, rate_type) {
  plot <- data %>% 
    ggplot() +
    geom_hline(yintercept=doe_fsc_ranges_avg) +
    theme_classic() +
    ggtitle(paste0(rate_type, ' Fuel Rates vs Average Projected Range (2019 - present, enriched beetlejuice load data)')) +
    labs(x=' ', y='Fuel RPM')
  
  return(plot)
}

# Mode
mode_plot <- plotter_template(combined_mode_df, 'Mode') +
  geom_line(aes(ds, fuel_rpm, color=mode_type))
mode_boxplot <- boxplotter_template(combined_mode_df, 'Mode') +
  geom_boxplot(aes(mode_type, fuel_rpm))

mode_plot
mode_boxplot

# PADD
PADD_plot <- plotter_template(combined_PADD_df, 'PADD') +
  geom_line(aes(ds, fuel_rpm, color=PADD_type))
PADD_boxplot <- boxplotter_template(combined_PADD_df, 'PADD') +
  geom_boxplot(aes(PADD_type, fuel_rpm))
# PADD_boxplot_mode <- boxplotter_template(combined_PADD_df, 'PADD') +
#   geom_boxplot(aes(mode_type, fuel_rpm)) +
#   facet_wrap('PADD_type')

PADD_plot
PADD_boxplot
# PADD_boxplot_mode

# Region
region_plot <- plotter_template(combined_region_df, 'Region') +
  geom_line(aes(ds, fuel_rpm, color=lane_region))
region_boxplot <- boxplotter_template(combined_region_df, 'Region') +
  geom_boxplot(aes(lane_region, fuel_rpm)) +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
# region_boxplot_mode <- boxplotter_template(combined_region_df, 'Region') +
#   geom_boxplot(aes(mode_type, fuel_rpm)) +
#   facet_wrap('lane_region')

region_plot
region_boxplot
# region_boxplot_mode

# LOH
LOH_plot <- plotter_template(combined_LOH_df, 'LOH') +
  geom_line(aes(ds, fuel_rpm, color=LOH_type))
LOH_boxplot <- boxplotter_template(combined_LOH_df, 'LOH') +
  geom_boxplot(aes(LOH_type, fuel_rpm))
# LOH_boxplot_mode <- boxplotter_template(combined_LOH_df, 'LOH') +
#   geom_boxplot(aes(mode_type, fuel_rpm)) +
#   facet_wrap('LOH_type')

LOH_plot
LOH_boxplot
# LOH_boxplot_mode
