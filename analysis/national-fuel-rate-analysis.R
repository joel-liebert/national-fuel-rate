#---- import packages ----
library(bigrquery)
library(DBI)
library(dplyr)
library(ggplot2)
library(tidyr)
library(stringr)

#---- load data ----
bqcon <- dbConnect(
  bigrquery::bigquery(),
  project = "freightwaves-data-factory"
)

query1 = 
  "select
     *
   from `freightwaves-data-factory.data_product_contract_rates_inverse_weighting.contract_rate_output_table` tablesample system (25 percent)
   where loh_group='overall' 
    and peer_group='market'
    and date='2022-11-06'"

query2 = 
  "with sums as (
      select
        data_timestamp + 1 as data_timestamp
        ,mode
        ,cast( substr(od_pair, 1, 3) as int64 ) as origin_zip3
        ,cast( substr(od_pair, 5, 3) as int64 ) as dest_zip3
        ,substr(od_pair, 1, 1) as origin_zip1
        ,substr(od_pair, 5, 1) as dest_zip1
        ,effective_fuel_charge
        ,effective_miles
      from `freightwaves-data-factory.output_trucking_rates.contract_output_history` tablesample system (25 percent)
      where data_timestamp = '2022-11-13'
    ),
    PADD as (
      select
        data_timestamp
        ,mode
        ,origin_zip1
        ,dest_zip1
        ,case 
          when origin_zip3  > 888 and dest_zip3  > 888 then 'full PADD5'
          when origin_zip3  > 888 or  dest_zip3  > 888 then 'half PADD5'
          when origin_zip3 <= 888 and dest_zip3 <= 888 then 'not PADD5'
          end as PADD_type
        ,effective_fuel_charge
        ,effective_miles
      from sums
    ),
    regions as (
      select
        data_timestamp
        ,mode
        ,PADD_type
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
        ,effective_fuel_charge
        ,effective_miles
      from PADD
    ),
    lane_regions as (
      select
        data_timestamp
        ,mode
        ,PADD_type
        ,concat( greatest(orig_region, dest_region), '-', least(orig_region, dest_region) ) as lane_region
        ,effective_fuel_charge
        ,effective_miles
      from regions
    ),
    LOH as (
      select
        data_timestamp
        ,mode
        ,PADD_type
        ,lane_region
        ,case 
          when effective_miles < 100 then 'city'
          when effective_miles < 250 then 'short'
          when effective_miles < 500 then 'mid'
          when effective_miles < 750 then 'tweener'
          when effective_miles < 1000 then 'long'
          else 'extra-long'
          end as LOH_type
        ,round( safe_divide(effective_fuel_charge, effective_miles), 2 ) as fuel_rpm
      from lane_regions
    )
    
    select * from LOH
    order by data_timestamp, PADD_type, lane_region, LOH_type, fuel_rpm"

contract_df <- dbGetQuery(bqcon, query1)
contract_df2 <- dbGetQuery(bqcon, query2)

#---- create estimated boundaries ----
# set up matrix
mpg = c(5, 6, 7)
fuel_charge_per_gallon = c(1, 1.5, 2)
fuel_costs_per_gallon  = c(4.78, 5.10, 5.39)

df1 <- tibble(mpg, fuel_charge_per_gallon, fuel_costs_per_gallon)

combos <- df1 %>% 
  expand(mpg, fuel_costs_per_gallon, fuel_charge_per_gallon) %>%
  mutate(fuel_charge_per_miles    = fuel_charge_per_gallon/mpg,
         fuel_surcharge_per_miles = (fuel_costs_per_gallon - fuel_charge_per_gallon)/mpg,
         fuel_rpm = fuel_charge_per_miles + fuel_surcharge_per_miles)

fuel_rpm_vector <- c(low=min(combos$fuel_rpm),
                     mid=mean(combos$fuel_rpm),
                     high=max(combos$fuel_rpm)) %>% print()

fuel_charge_vector <- c(low=min(combos$fuel_charge_per_miles),
                        mid=mean(combos$fuel_charge_per_miles),
                        high=max(combos$fuel_charge_per_miles)) %>% print()

fuel_surcharge_vector <- c(low=min(combos$fuel_surcharge_per_miles),
                           mid=mean(combos$fuel_surcharge_per_miles),
                           high=max(combos$fuel_surcharge_per_miles)) %>% print()

temp_FSC_vector <- c(low=doe_FW_ranges[doe_FW_ranges$ds=='2022-11-14',]$value[1],
                     mid=doe_FW_ranges[doe_FW_ranges$ds=='2022-11-14',]$value[2],
                     high=doe_FW_ranges[doe_FW_ranges$ds=='2022-11-14',]$value[3])

#---- visualize distribution of fuel_rpm ----
fuel_rpm_distribution_plotter <- function(data, hline, facet_wrap) {
  
  plot <- data %>% 
    ggplot(aes(mode, fuel_rpm)) +
    geom_boxplot() +
    geom_hline(yintercept=hline) +
    theme_classic() +
    facet_wrap(facet_wrap)
  
  return(plot)
  
}

rpm_df <- contract_df %>% 
  drop_na() %>% 
  # mutate(PADD = factor(case_when(as.integer(origin_zip3) > 888 & as.integer(dest_zip3) > 888 ~ 'full PADD5',
  #                                as.integer(origin_zip3) > 888 | as.integer(dest_zip3) > 888 ~ 'half PADD5',
  #                                TRUE ~ 'not PADD5')),
  mutate(PADD = case_when(as.integer(origin_zip3) > 888 ~ 'PADD5',
                          as.integer(dest_zip3)   > 888 ~ 'PADD5',
                          TRUE ~ 'not PADD5'),
         orig_region = case_when(substr(origin_zip3, 1, 1) %in% c(0,1,2) ~ 'atlantic',
                                 substr(origin_zip3, 1, 1) %in% c(3,7)   ~ 'south',
                                 substr(origin_zip3, 1, 1) %in% c(4,5,6) ~ 'midwest',
                                 substr(origin_zip3, 1, 1) %in% c(8,9)   ~ 'west'),
         dest_region = case_when(substr(dest_zip3, 1, 1) %in% c(0,1,2) ~ 'atlantic',
                                 substr(dest_zip3, 1, 1) %in% c(3,7)   ~ 'south',
                                 substr(dest_zip3, 1, 1) %in% c(4,5,6) ~ 'midwest',
                                 substr(dest_zip3, 1, 1) %in% c(8,9)   ~ 'west'),
         lane_region = paste0( pmin(orig_region, dest_region), '-', pmax(orig_region, dest_region) ),
         LOH = case_when(effective_miles < 100  ~ 'city',
                         effective_miles < 250  ~ 'short',
                         effective_miles < 500  ~ 'mid',
                         effective_miles < 750  ~ 'tweener',
                         effective_miles < 1000 ~ 'long',
                         TRUE ~ 'extra-long'),
         LOH = factor(LOH, levels=c('city','short','mid','tweener','long','extra-long')),
         fuel_rpm = round( effective_fuel_charge/effective_miles, 2 ))

contract_df2 <- contract_df2 %>% 
  mutate(LOH_type = factor(LOH_type, levels=c('city','short','mid','tweener','long','extra-long')))

fuel_rpm_distribution_plotter(rpm_df, fuel_surcharge_vector, 'Mode ') +
  ggtitle("Contract Fuel Rates Plotted Against Low, Mid, & High Expectations") +
  labs(x='Mode', y='Fuel RPM')

mode_boxplot <- contract_df2 %>% 
  ggplot(aes(mode, fuel_rpm)) +
  geom_boxplot() +
  geom_hline(yintercept=temp_FSC_vector) +
  theme_classic() +
  ggtitle("Mode Fuel Rates vs Projected Range (2022-11-14)") +
  labs(x=' ', y='Fuel RPM')
mode_boxplot

PADD_boxplot <- contract_df2 %>% 
  ggplot(aes(mode, fuel_rpm)) +
  geom_boxplot() +
  geom_hline(yintercept=temp_FSC_vector) +
  facet_wrap('PADD_type') +
  theme_classic() +
  ggtitle("PADD Fuel Rates vs Projected Range (2022-11-14)") +
  labs(x=' ', y='Fuel RPM')
PADD_boxplot

region_boxplot <- contract_df2 %>% 
  ggplot(aes(mode, fuel_rpm)) +
  geom_boxplot() +
  geom_hline(yintercept=temp_FSC_vector) +
  facet_wrap('lane_region') +
  theme_classic() +
  ggtitle("Region Fuel Rates vs Projected Range (2022-11-14)") +
  labs(x=' ', y='Fuel RPM')
region_boxplot

LOH_boxplot <- contract_df2 %>% 
  ggplot(aes(mode, fuel_rpm)) +
  geom_boxplot() +
  geom_hline(yintercept=temp_FSC_vector) +
  facet_wrap('LOH_type') +
  theme_classic() +
  ggtitle("LOH Fuel Rates vs Projected Range (2022-11-14)") +
  labs(x=' ', y='Fuel RPM')
LOH_boxplot


PADD_boxplot_no_mode <- contract_df2 %>% 
  ggplot(aes(PADD_type, fuel_rpm)) +
  geom_boxplot() +
  geom_hline(yintercept=temp_FSC_vector) +
  theme_classic() +
  ggtitle("PADD Fuel Rates vs Projected Range (2022-11-14)") +
  labs(x=' ', y='Fuel RPM')
PADD_boxplot_no_mode

region_boxplot_no_mode <- contract_df2 %>% 
  ggplot(aes(lane_region, fuel_rpm)) +
  geom_boxplot() +
  geom_hline(yintercept=temp_FSC_vector) +
  theme_classic() +
  ggtitle("Region Fuel Rates vs Projected Range (2022-11-14)") +
  labs(x=' ', y='Fuel RPM') +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
region_boxplot_no_mode

LOH_boxplot_no_mode <- contract_df2 %>% 
  ggplot(aes(LOH_type, fuel_rpm)) +
  geom_boxplot() +
  geom_hline(yintercept=temp_FSC_vector) +
  theme_classic() +
  ggtitle("LOH Fuel Rates vs Projected Range (2022-11-14)") +
  labs(x=' ', y='Fuel RPM')
LOH_boxplot_no_mode

#---- history ----
# query2 = 
#   "select
#      data_timestamp, od_pair, mode, lane_regions, effective_miles, effective_fuel_charge
#    from `freightwaves-data-factory.output_trucking_rates.contract_output_history`
#    where data_timestamp > '2022-05-31'"
# history_df <- dbGetQuery(bqcon, query2)

# rpm_history_df <- history_df %>% 
#   drop_na() %>% 
#   mutate(origin_zip3 = substr(od_pair, 1, 3),
#          dest_zip3   = substr(od_pair, 5, 7),
#          PADD = case_when(as.integer(origin_zip3) > 888 ~ 'PADD5',
#                           as.integer(dest_zip3)   > 888 ~ 'PADD5',
#                           TRUE ~ 'not PADD5'),
#          orig_region = case_when(substr(origin_zip3, 1, 1) %in% c(0,1,2) ~ 'atlantic',
#                                  substr(origin_zip3, 1, 1) %in% c(3,7)   ~ 'south',
#                                  substr(origin_zip3, 1, 1) %in% c(4,5,6) ~ 'midwest',
#                                  substr(origin_zip3, 1, 1) %in% c(8,9)   ~ 'west'),
#          dest_region = case_when(substr(dest_zip3, 1, 1) %in% c(0,1,2) ~ 'atlantic',
#                                  substr(dest_zip3, 1, 1) %in% c(3,7)   ~ 'south',
#                                  substr(dest_zip3, 1, 1) %in% c(4,5,6) ~ 'midwest',
#                                  substr(dest_zip3, 1, 1) %in% c(8,9)   ~ 'west'),
#          lane_region = paste0( pmin(orig_region, dest_region), '-', pmax(orig_region, dest_region) ),
#          LOH = case_when(effective_miles < 100  ~ 'city',
#                          effective_miles < 250  ~ 'short',
#                          effective_miles < 500  ~ 'mid',
#                          effective_miles < 750  ~ 'tweener',
#                          effective_miles < 1000 ~ 'long',
#                          TRUE ~ 'extra-long'),
#          LOH = factor(LOH, levels=c('city','short','mid','tweener','long','extra-long')),
#          fuel_rpm = round( effective_fuel_charge/effective_miles, 2 ))

# fuel_rpm_distribution_plotter(rpm_history_df, fuel_surcharge_vector, 'data_timestamp') +
# ggtitle("History")
