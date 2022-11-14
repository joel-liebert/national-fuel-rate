#---- import packages ----
library(googleCloudStorageR)
library(bigrquery)
library(dplyr)
library(tidyr)
library(zoo)

source('./utils.R')

#---- load data ----
doe_FW <- response_pull(data_to_forecast='DOE', type='ticker')

bqcon <- dbConnect(
  bigrquery::bigquery(),
  project = "freightwaves-data-factory"
)

query1 = 
  "with sums as (
      select
        data_timestamp + 1 as data_timestamp
        ,mode
        ,sum(effective_miles) as effective_miles_sum
        ,sum(effective_fuel_charge) as effective_fuel_charge_sum
      from `freightwaves-data-factory.output_trucking_rates.contract_output_history`
      where data_timestamp > '2019-01-01'
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

contract_df <- dbGetQuery(bqcon, query1)

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
combined_df <- doe_FW_ranges %>% 
  left_join(contract_df, by=c('ds'='data_timestamp'))

#---- plot data ----
combined_plot <- combined_df %>%
  ggplot(aes(ds, value, fill=rate)) +
  geom_line() +
  geom_line(aes(ds, fuel_rpm, color=mode)) +
  theme_classic() +
  labs(x='', y='Fuel RPM') +
  ggtitle('Contract Fuel Rates vs Projected Range (based on ten-week rolling average of DOE.USA)')
combined_plot
