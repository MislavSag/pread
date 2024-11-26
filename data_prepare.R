library(data.table)
library(arrow)
library(dplyr)
library(qlcal)
library(ggplot2)


# SET UP ------------------------------------------------------------------
# Global vars
PATH         = "/home/sn/data/equity/us"
PATH_DATASET = "/home/sn/data/strategies/pread"

# Set NYSE calendar
setCalendar("UnitedStates/NYSE")

# Constants
update = TRUE

# EARING ANNOUNCEMENT DATA ------------------------------------------------
# get events data
events = read_parquet(fs::path(PATH, "fundamentals", "earning_announcements", ext = "parquet"))

# Plot number of rows by date
ggplot(events[, .N, by = date][order(date)], aes(x = date, y = N)) +
  geom_line() +
  theme_minimal()

# Coarse filtering 
# IMPORTANT for LIVE: If we execute script after market close here 1, if day after then 2
if (data.table::hour(Sys.time()) < 12) {
  day_gap = 1
} else {
  day_gap = 2
}
events = events[date <= advanceDate(Sys.Date(), day_gap)]

# Remove duplicates
events[, .N, by = c("symbol", "date")][N > 1]
events = unique(events, by = c("symbol", "date"))  # remove duplicated symbol / date pair

# Get investing.com data
investingcom_ea = read_parquet(
  fs::path(
    PATH,
    "fundamentals",
    "earning_announcements_investingcom",
    ext = "parquet"
  )
)

# Keep columns we need and convert date and add col names prefix
investingcom_ea = investingcom_ea[, .(symbol, time, eps, eps_forecast, revenue, revenue_forecast, right_time)]
investingcom_ea[, date_investingcom := as.Date(time)]
investingcom_ea = investingcom_ea[date_investingcom <= qlcal::advanceDate(Sys.Date(), 2)]
setnames(investingcom_ea, 
         colnames(investingcom_ea)[2:6],
         paste0(colnames(investingcom_ea)[2:6], "_investingcom"))

# Plot number of rows by date
ggplot(investingcom_ea[, .N, by = date_investingcom][order(date_investingcom)], 
       aes(x = date_investingcom, y = N)) +
  geom_line() +
  theme_minimal()

setorder(investingcom_ea, date_investingcom)
investingcom_ea[date_investingcom %between% c("2024-11-01", "2024-11-08")][!is.na(eps_investingcom)]

# Get earnings surprises data from FMP
es = read_parquet(
  fs::path(
    PATH,
    "fundamentals",
    "earning_surprises",
    ext = "parquet"
  )
)
ggplot(es[, .N, by = date][order(date)], aes(x = date, y = N)) +
  geom_line() +
  theme_minimal()

# merge DT and investing com earnings surprises
events = merge(
  events,
  investingcom_ea,
  by.x = c("symbol", "date"),
  by.y = c("symbol", "date_investingcom"),
  all.x = TRUE,
  all.y = FALSE
)
events = merge(events, es, by = c("symbol", "date"), all = TRUE)
  
# keep only observations available in both datasets by checking dates
events = events[!is.na(date) & !is.na(as.Date(time_investingcom))]

# Check if time are the same
events[!is.na(right_time) & right_time == "marketClosed ", right_time := "amc"]
events[!is.na(right_time) & right_time == "marketOpen ", right_time := "bmo"]
events[, same_announce_time := time == right_time]

# if both fmp cloud and investing.com data exists keep similar
print(paste0("Number of removed observations because time of announcements are not same :",
             sum(!((events$same_announce_time) == TRUE), na.rm = TRUE), " or ",
             round(sum(!((events$same_announce_time) == TRUE), na.rm = TRUE) / nrow(events), 4) * 100, "% percent."))
events = events[events$same_announce_time == TRUE]

# Remove duplicated events
events = unique(events, by = c("symbol", "date"))

# Keep only rows with similar eps at least in one of datasets (investing com of sueprises in FMP)
eps_threshold = 0.1
events_ic_similar  = events[(eps >= eps_investingcom * 1-eps_threshold) & eps <= (1 + eps_threshold)]
events_fmp_similar = events[(eps >= actualEarningResult * 1-eps_threshold) & actualEarningResult <= (1 + eps_threshold)]
events = unique(rbind(events_ic_similar, 
                      events_fmp_similar, 
                      events[date > (Sys.Date() - 1)]))

# Checks
events[, max(date)] # last date
events[date == max(date), symbol] # Symbols for last date

# Plot number of rows by date
ggplot(events[, .N, by = date][order(date)], aes(x = date, y = N)) +
  geom_line() +
  theme_minimal()

# MARKET DATA AND FUNDAMENTALS ---------------------------------------------
# Get factors
path_to_parquet = fs::path(PATH, "predictors_daily", "factors", "prices_factors.parquet")
events_symbols = c(events[, unique(symbol)], "SPY")
prices_dt = open_dataset(path_to_parquet, format = "parquet") |>
  dplyr::filter(date > "2008-01-01", symbol %in% events_symbols) |>
  dplyr::arrange(symbol, date) |>
  collect() |>
setDT(prices_dt)

# Checks and summarizies
prices_dt[, max(date, na.rm = TRUE)]

# Filter dates and symbols
prices_dt = unique(prices_dt, by = c("symbol", "date"))
prices_n = prices_dt[, .N, by = symbol]
prices_n = prices_n[which(prices_n$N > 700)]  # remove prices with only 700 or less observations
prices_dt = prices_dt[symbol %chin% prices_n[, symbol]]

# Remove symbols that have (almost) constant close prices
prices_dt[, sd_roll := roll::roll_sd(close, 22 * 6), by = symbol]
symbols_remove = prices_dt[sd_roll == 0, unique(symbol)]
prices_dt = prices_dt[symbol %notin% symbols_remove]

# SPY data
spy = open_dataset("/home/sn/data/equity/daily_fmp_all.csv", format = "csv") |>
  dplyr::filter(symbol == "SPY") |>
  dplyr::select(date, adjClose) |>
  dplyr::rename(close = adjClose) |>
  collect()
setDT(spy)
spy[, returns := close / shift(close) - 1]
spy = na.omit(spy)  

# Free memory
gc()

# Check if dates form events are aligned with dates from prices
# This checks should be done after market closes, but my data is updated after 
# 00:00, so take this into account
events[, max(date)]
last_trading_day = events[, data.table::last(sort(unique(date)), 3)[1]]
last_trading_day_corected = events[, data.table::last(sort(unique(date)), 4)[1]]
prices_dt[, max(date)]
prices_dt[date == last_trading_day]
prices_dt[date == last_trading_day_corected]

# LABELING ----------------------------------------------------------
# Labeling depending on strategy
setorder(prices_dt, symbol, date)

# Calculate returns
prices_dt[, amc_return := shift(open, -1L, "shift") / close - 1, by = "symbol"]
prices_dt[, bmo_return := open / shift(close) - 1, by = "symbol"]
target_variables = c("amc_return", "bmo_return")

# Merge events and price data
remove_cols = c("open", "high", "low", "close", "volume", "close_raw", "returns")
cols_keep = setdiff(colnames(prices_dt), remove_cols)

# events that are in the future. If I use merge, it want merge newest data.
events[, date_event := date] 
prices_dt[, date_prices := date]
dataset = prices_dt[, .SD, .SDcols = c("date_prices", cols_keep)][events, on = c("symbol", "date"), roll = Inf]

# Checks
dataset[, .(date, date_event, date_prices)]
dataset[ date_event != date_prices, .(date, date_event, date_prices)]
events[, max(date)]
last_trading_day = events[, data.table::last(sort(unique(date)), 3)[1]]
last_trading_day_corected = events[, data.table::last(sort(unique(date)), 4)[1]]
prices_dt[, max(date)]
prices_dt[date == last_trading_day]
prices_dt[date == last_trading_day_corected]
symbols_ = dataset[date == max(date), symbol]
prices_dt[date == max(date) & symbol %in% symbols_, .SD, .SDcols = c("date", "symbol", "maxret")]
dataset[date == max(date), .SD, .SDcols = c("date", "symbol", "maxret")]
dataset[, max(date_prices, na.rm = TRUE)]
dataset[date_prices == max(date_prices, na.rm = TRUE), .SD, .SDcols = c("date", "symbol", "maxret")]
cols_ = c("date", "symbol", "maxret", "indmom")
dataset[date == last_trading_day_corected, .SD, , .SDcols = cols_]

# Save every symbol separately
dataset_dir = file.path(PATH_DATASET, "dataset")
if (!dir.exists(dataset_dir)) {
  dir.create(dataset_dir)
}
prices_dir = file.path(PATH_DATASET, "prices")
if (!dir.exists(prices_dir)) {
  dir.create(prices_dir)
}
for (s in dataset[, unique(symbol)]) {
  dt_ = dataset[symbol == s]
  prices_ = prices_dt[symbol == s]
  if (nrow(dt_) == 0 | nrow(prices_) == 0) next
  file_name = file.path(dataset_dir, paste0(s, ".csv"))
  fwrite(dt_, file_name)
  file_name = file.path(prices_dir, paste0(s, ".csv"))
  fwrite(prices_, file_name)
}

# Create sh file for predictors
cont = sprintf(
"#!/bin/bash

#PBS -N pread_predictions
#PBS -l ncpus=1
#PBS -l mem=4GB
#PBS -J 1-%d
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}

apptainer run image_predictors.sif predictors_padobran.R",
length(list.files(dataset_dir)))
writeLines(cont, "predictors_padobran.sh")

# Add to padobran
# scp -r /home/sn/data/strategies/pread/dataset/ padobran:/home/jmaric/pread/dataset
# scp -r /home/sn/data/strategies/pread/prices padobran:/home/jmaric/pread/prices
