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

contract_df <- dbGetQuery(bqcon, query1)

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

fuel_rpm_distribution_plotter(rpm_df, fuel_surcharge_vector, 'LOH') +
  ggtitle("Contract Fuel Rates Plotted Against Low, Mid, & High Expectations") +
  labs(x='Mode', y='Fuel RPM')

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
