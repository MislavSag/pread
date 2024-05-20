library(data.table)
library(arrow)
library(qlcal)
library(duckdb)


# SET UP ------------------------------------------------------------------
# Global vars
PATH         = "/home/sn/data/equity/us"
PATH_DATASET = "/home/sn/data/strategies/pread"

# Set NYSE calendarr
setCalendar("UnitedStates/NYSE")

# Constants
update = TRUE


# EARING ANNOUNCEMENT DATA ------------------------------------------------
# get events data
events = read_parquet(fs::path(PATH, "fundamentals", "earning_announcements", ext = "parquet"))

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

# merge DT and investing com earnings surprises
events = merge(
  events,
  investingcom_ea,
  by.x = c("symbol", "date"),
  by.y = c("symbol", "date_investingcom"),
  all.x = TRUE,
  all.y = FALSE
)
  
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

# Check last date
events[, max(date)]
events[date == max(date), symbol]


# MARKET DATA AND FUNDAMENTALS ---------------------------------------------
# Initialize connection to DuckDB
con = dbConnect(duckdb::duckdb())
path_to_parquet = fs::path(PATH, "predictors_daily", "factors", "prices_factors.parquet")
events_symbols = c(events[, unique(symbol)], "SPY")
query <- sprintf("
  SELECT *
  FROM read_parquet('%s')
  WHERE date > '2008-01-01'
    AND symbol IN (%s)
  ORDER BY symbol, date
", path_to_parquet, paste(shQuote(events_symbols), collapse = ", "))
prices_dt = dbGetQuery(con, query)
setDT(prices_dt)
duckdb::dbDisconnect(con)

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
con = dbConnect(duckdb::duckdb())
path_ = "/home/sn/data/equity/daily_fmp_all.csv"
query <- sprintf("
  SELECT *
  FROM read_csv_auto('%s', sample_size=-1)
  WHERE Symbol = '%s'
", path_, "SPY")
data_ <- dbGetQuery(con, query)
duckdb::dbDisconnect(con)
setDT(data_)
data_ = data_[, .(date = date, close = adjClose)]
data_[, returns := close / shift(close) - 1]
spy = na.omit(data_)

# Free memory
gc()

# Check if dates form events are aligned with dates from prices
# This checks should be done after market closes, but my data is updated after 
# 00:00, so take this into account
events[, max(date)]
last_trading_day = events[, last(sort(unique(date)), 3)[1]]
last_trading_day_corected = events[, last(sort(unique(date)), 4)[1]]
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

# events that are in the future. If I use merge, it want merge newst data.
events[, date_event := date] 
prices_dt[, date_prices := date]
dataset = prices_dt[, .SD, .SDcols = c("date_prices", cols_keep)][events, on = c("symbol", "date"), roll = Inf]

# Checks
dataset[, .(date, date_event, date_prices)]
dataset[ date_event != date_prices, .(date, date_event, date_prices)]
events[, max(date)]
last_trading_day = events[, last(sort(unique(date)), 3)[1]]
last_trading_day_corected = events[, last(sort(unique(date)), 4)[1]]
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

# Save dataset and prices locally
file_name = file.path(PATH_DATASET, "dataset_pread.csv")
fwrite(dataset, file_name)
file_name_p = file.path(PATH_DATASET, "prices_pread.csv")
fwrite(prices_dt, file_name_p)

# Mannually add to padobran
# I have to enter password when exwcuted, not sure if it is possible to automate this
# if (!update) {
#   scp_command = "scp /home/sn/data/strategies/pread/dataset_pread.csv padobran:/home/jmaric/pread/dataset_pread.csv"
#   system(scp_command)
#   scp_command = "scp /home/sn/data/strategies/pread/prices_pread.csv padobran:/home/jmaric/pread/prices_pread.csv"
# }
