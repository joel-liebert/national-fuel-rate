#---- Pull Data from API ----
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