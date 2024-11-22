options(progress_enabled = FALSE)

# remotes::install_github("MislavSag/finfeatures")
library(data.table)
library(fs)
library(finfeatures)
library(glue)
library(reticulate)
library(janitor)
library(arrow)
library(AzureStor)


# SETUP -------------------------------------------------------------------
# Paths
PATH                  = "/home/sn/data/strategies/pread"
PATH_ROLLING          = path(PATH, "predictors")
PATH_ROLLING_PADOBRAN = path(PATH, "predictors_padobran")
PATH_PREDICTORS       = "/home/sn/data/equity/us/predictors_daily/pead_predictors/"

# Create directories
if (!dir_exists(PATH_ROLLING)) dir_create(PATH_ROLLING)

# Python environment
reticulate::use_python("/home/sn/quant/bin/python3", required = TRUE)
builtins = import_builtins(convert = FALSE)
main = import_main(convert = FALSE)
tsfel = reticulate::import("tsfel", convert = FALSE)
warnigns = reticulate::import("warnings", convert = FALSE)
warnigns$filterwarnings('ignore')

# Define target variables
target_variables = c("amc_return", "bmo_return")

# Utils
# Move this to finfeatures
clean_col_names = function(names) {
  names = gsub(" |-|\\.|\"", "_", names)
  names = gsub("_{2,}", "_", names)
  names
} 

# PRICES AND EVENTS -------------------------------------------------------
# Get data
dataset = lapply(list.files(file.path(PATH, "dataset"), full.names = TRUE), fread)
dataset = rbindlist(dataset)
ohlcv = lapply(list.files(file.path(PATH, "prices"), full.names = TRUE), fread)
ohlcv = rbindlist(ohlcv)
ohlcv = Ohlcv$new(ohlcv[, .(symbol, date, open, high, low, close, volume)], 
                  date_col = "date")

# ROLLING PREDICTORS ------------------------------------------------------
# Command to get data from padobran. This is necessary only first time
# scp -r padobran:/home/jmaric/pread/predictors_padobran/ /home/sn/data/strategies/pread/

# Clean padobran data
fnames = unique(gsub("-.*", "", path_file(dir_ls(PATH_ROLLING_PADOBRAN))))
fnames_exist = path_ext_remove(path_file(dir_ls(PATH_ROLLING)))  
fnames = setdiff(fnames, fnames_exist)
if (length(fnames) > 0) {
  lapply(fnames, function(x) {
    dt_ = lapply(dir_ls(PATH_ROLLING_PADOBRAN, regexp = x), fread)
    dt_ = rbindlist(dt_, fill = TRUE)
    fwrite(dt_, path(PATH_ROLLING, paste0(x, ".csv")))
  })
}

# Parameters
workers = 2L
windows = c(66, 252) # day and 2H;  cca 10 days

# Define at parameter
ohlcv_max_date = ohlcv$X[, max(date)]
get_at = function(n = "exuber.csv", remove_old = TRUE) {
  # n = "backcusum.csv"
  if (is.null(n)) {
    new_dataset = dataset[, .(symbol, date = as.IDate(date))]
  } else {
    predictors_ = fread(path(PATH_ROLLING, n))
    new_dataset = fsetdiff(dataset[, .(symbol, date = as.IDate(date))],
                           predictors_[, .(symbol, date)])
  }
  new_dataset[, date_merge := fifelse(date > ohlcv_max_date, ohlcv_max_date, date)]
  new_data = merge(
    ohlcv$X[, .(symbol, date, date_ohlcv = date)],
    new_dataset[, .(symbol, date_event = date, date = date_merge, index = TRUE)],
    by = c("symbol", "date"),
    all.x = TRUE,
    all.y = FALSE
  )
  new_data[, n := 1:nrow(new_data)]
  new_data = new_data[index == TRUE, .(symbol, date_ohlcv, date_event, n)]
  new_data[, lags := 2L]
  new_data[date_event > date_ohlcv, lags := 0L]
  
  # Skip everything before padobran
  if (remove_old) {
    new_data[date_ohlcv > as.Date("2024-11-01")]
  }
  return(new_data)
}

# Exuber
n_ = "exuber.csv"
meta = get_at(n_)
windows_ = c(windows, 504)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingExuber$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    exuber_lag = 1L
  )
  new = predictors_init$get_rolling_features(ohlcv, TRUE)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingExuber$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    exuber_lag = 1L
  )
  new = predictors_init$get_rolling_features(ohlcv, TRUE)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Backcusum
n_ = "backcusum.csv"
meta = get_at(n_)
windows_ = c(windows, 504)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingBackcusum$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    alternative = c("greater", "two.sided"),
    return_power = c(1, 2)
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingBackcusum$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    alternative = c("greater", "two.sided"),
    return_power = c(1, 2)
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Forecasts
n_ = "forecasts.csv"
meta = get_at(n_)
windows_ = c(252, 252 * 2)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingForecats$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    forecast_type = c("autoarima", "nnetar", "ets"),
    h = 22
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingForecats$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    forecast_type = c("autoarima", "nnetar", "ets"),
    h = 22
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Theft r
n_ = "theftr.csv"
meta = get_at(n_)
windows_ = c(5, 22, windows)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    features_set = c("catch22", "feasts")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new, fill = TRUE)
  new[, c("feasts____22_5", "feasts____25_22") := NULL]
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    features_set = c("catch22", "feasts")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Theft r with returns
n_ = "theftrr.csv"
meta = get_at(n_)
windows_ = c(5, 22, windows)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    features_set = c("catch22", "feasts")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new, fill = TRUE)
  new[, c("feasts____22_5", "feasts____25_22") := NULL]
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    features_set = c("catch22", "feasts")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Tsfeatures
n_ = "tsfeatures.csv"
meta = get_at(n_)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingTsfeatures$new(
    windows = windows,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    scale = TRUE
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTsfeatures$new(
    windows = windows,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    scale = TRUE
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# WaveletArima
n_ = "waveletarima.csv"
meta = get_at(n_)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingWaveletArima$new(
    windows = windows,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    filter = "haar"
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingWaveletArima$new(
    windows = windows,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    filter = "haar"
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# FracDiff
n_ = "fracdiff.csv"
meta = get_at(n_)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingFracdiff$new(
    windows = windows,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    nar = c(1), 
    nma = c(1),
    bandw_exp = c(0.1, 0.5, 0.9)
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingFracdiff$new(
    windows = windows,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    nar = c(1), 
    nma = c(1),
    bandw_exp = c(0.1, 0.5, 0.9)
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Theft py
n_ = "theftpy.csv"
meta = get_at(n_)
windows_ = c(22, windows)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    features_set = c("tsfel", "tsfresh")
  )
  new = suppressMessages({predictors_init$get_rolling_features(ohlcv)})
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  colnames(old) = clean_col_names(colnames(old))
  colnames(new) = clean_col_names(colnames(new))
  new = rbind(old, new, fill = TRUE)
  new = unique(new, by = c("symbol", "date"))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    features_set = c("tsfel", "tsfresh")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  colnames(new) = clean_col_names(colnames(new))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Theft py with returns
n_ = "theftpyr.csv"
meta = get_at(n_)
windows_ = c(22, windows)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    features_set = c("tsfel", "tsfresh")
  )
  new = suppressMessages({predictors_init$get_rolling_features(ohlcv)})
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  colnames(old) = clean_col_names(colnames(old))
  colnames(new) = clean_col_names(colnames(new))
  new = rbind(old, new, fill = TRUE)
  new = unique(new, by = c("symbol", "date"))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    features_set = c("tsfel", "tsfresh")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  colnames(new) = clean_col_names(colnames(new))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Vse
n_ = "vse.csv"
meta = get_at(n_)
windows_ = c(22, windows)
if (meta[, any(lags == 2)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 2, n],
    lag = 2L,
    features_set = c("tsfel", "tsfresh")
  )
  new = suppressMessages({predictors_init$get_rolling_features(ohlcv)})
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  colnames(old) = clean_col_names(colnames(old))
  colnames(new) = clean_col_names(colnames(new))
  new = rbind(old, new, fill = TRUE)
  new = unique(new, by = c("symbol", "date"))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    features_set = c("tsfel", "tsfresh")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  colnames(new) = clean_col_names(colnames(new))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Prepare features for merge
rolling_predictors = lapply(fnames[!grepl("live", fnames)], fread)
names(rolling_predictors) = gsub("\\.csv", "", basename(fnames[!grepl("live", fnames)]))
colnames(rolling_predictors[["theftrr"]])[-(1:2)] = paste0(colnames(rolling_predictors[["theftrr"]])[-(1:2)], 
                                                           "_returns") 
colnames(rolling_predictors[["theftpyr"]])[-(1:2)] = paste0(colnames(rolling_predictors[["theftpyr"]])[-(1:2)], 
                                                           "_returns") 
rolling_predictors = lapply(rolling_predictors, function(dt_) {
  dt_[, !duplicated(names(dt_)), with = FALSE]
})

# Merge signals
rolling_predictors = Reduce(
  function(x, y) merge( x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
  rolling_predictors
)
rolling_predictors_new =  Reduce(
  function(x, y) merge( x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
  lapply(fnames[grepl("live", fnames)], fread)
)
rolling_predictors = rbind(rolling_predictors, rolling_predictors_new, fill = TRUE)
dim(rolling_predictors)

# Fix column names
rolling_predictors = clean_names(rolling_predictors)

# OHLCV PREDICTORS --------------------------------------------------------
# Define at parameter
at_meta = get_at(NULL, FALSE)
at_meta[, lags := 2L]
at_meta[date_event > date_ohlcv, lags := 0L]
keep_ohlcv = at_meta[, n] - at_meta[, lags]

# Features from OHLLCV
print("Calculate Ohlcv features.")
ohlcv_init = OhlcvFeaturesDaily$new(
  at = NULL,
  windows = c(5, 10, 22, 22 * 3, 22 * 6, 22 * 12, 22 * 12 * 2),
  quantile_divergence_window =  c(22, 22 * 3, 22 * 6, 22 * 12, 22 * 12 * 2)
)
ohlcv_features = ohlcv_init$get_ohlcv_features(copy(ohlcv$X))
setorderv(ohlcv_features, c("symbol", "date"))
ohlcv_features_sample = ohlcv_features[keep_ohlcv]

# CHECKS ------------------------------------------------------------------
### Importmant notes:
# 1. ohlcv_features_sample has date 2 trading days before event.
# 2. Rolling predictors have date column that is the same as event date, but
# the predictor is calculated for 2 trading days before
# 3. So, we have to merge ohlcv_features_sample with roling from xxx.

# Check if dates corresponds to above notes
symbol_ = "MSFT"
dataset[symbol == symbol_, head(.SD), .SDcols = c("symbol", "date")]
ohlcv_features_sample[symbol == symbol_, head(.SD), .SDcols = c("symbol", "date")]
rolling_predictors[symbol == symbol_, head(.SD), .SDcols = c("symbol", "date")]
# We can see that rolling predictors have the same date as dataset. That is date
# column in rolling predictors is the event date, that is date when earnings
# are released. But the ohlcv have date columns that is 2 trading days before.
# This doesn't mean that ohlcv and rolling predictors are calculated on different dates.
# They are bot calculated 2 days before.

# Check dates for new data
dataset[, max(date)]
ohlcv$X[, max(date)]
ohlcv_features[, max(date)]
ohlcv_features_sample[, max(date)]
rolling_predictors[, max(date)]
dataset[date == max(date), .(symbol, date)]

# Check dates before merge
colnames(rolling_predictors)[grep("date", colnames(rolling_predictors))]
colnames(dataset)[grep("date", colnames(dataset))]
colnames(ohlcv_features_sample)[grep("date", colnames(ohlcv_features_sample))]
dataset[symbol == symbol_, .(date, updatedFromDate, date_prices, date_event)]
rolling_predictors[symbol == symbol_, .(date)]
ohlcv_features_sample[symbol == symbol_, .(date)]
ohlcv_features_sample[, max(date)]


# MERGE PREDICTORS --------------------------------------------------------
# Merge OHLCV predictors and rolling predictors
rolling_predictors[, date_rolling := date]
ohlcv_features_sample[, date_ohlcv := date]
features = rolling_predictors[ohlcv_features_sample, on = c("symbol", "date"), roll = -Inf]

# Check again merging dates
features[symbol == symbol_, .(symbol, date_rolling, date_ohlcv, date)]
features[, max(date)]
features[date == max(date), .(symbol, date_rolling, date_ohlcv, date)]

# Check for duplicates
anyDuplicated(features, by = c("symbol", "date"))
anyDuplicated(features, by = c("symbol", "date_ohlcv"))
anyDuplicated(features, by = c("symbol", "date_rolling"))
features = features[!duplicated(features[, .(symbol, date_rolling)])]

# Check merge features and events
any(duplicated(dataset[, .(symbol, date)]))
any(duplicated(features[, .(symbol, date_rolling)]))
features[symbol == symbol_, .(symbol, date_rolling)]
dataset[symbol == symbol_, .(symbol, date)]
dataset[,  max(date)]
dataset[,  max(date_prices, na.rm = TRUE)]
features[, max(date)]
features[date == (max(date) - 1), .(date, date_ohlcv, date_rolling)]
features[date == max(date), .(date, date_ohlcv, date_rolling)]
dataset[date == max(date), .(date)]

# Merge features and events
features[, date_features := date]
dataset[, date_dataset := date]
features = dataset[features, on = c("symbol", "date" = "date_rolling"), roll = -Inf]

# Check all those dates
features[, .(date, date_event, date_ohlcv, date_prices, date_dataset)]
features[, all(date_dataset == date_event)]
# date_dataset and date_event are the same. They represent the date of the event
# that is the date of the earnings release.
features[, all(date_ohlcv == date_features)]
# date_ohlcv and date_features are the same. These are the date when predictions
# are calculated. These date should br 2 days before event date for PRE.
features[, all(date == date_prices, na.rm = TRUE)]
# date is equal date prices
features[date != date_prices, .(symbol, date, date_prices)]
features[date != date_dataset, .(symbol, date, date_prices, date_dataset)]
# dage dataset is real event date, while in date it is the same for all eept last date

# free memory
rm(ohlcv_features)
gc()

# FUNDAMENTALS ------------------------------------------------------------
# import fundamental factors
fundamentals = read_parquet(path(
  "/home/sn/data/equity/us",
  "predictors_daily",
  "factors",
  "fundamental_factors",
  ext = "parquet"
))

# Clean fundamentals
fundamentals = fundamentals[date > as.Date("2008-01-01")]
fundamentals[, acceptedDateTime := as.POSIXct(acceptedDate, tz = "America/New_York")]
fundamentals[, acceptedDate := as.Date(acceptedDateTime)]
fundamentals[, acceptedDateFundamentals := acceptedDate]
data.table::setnames(fundamentals, "date", "fundamental_date")
fundamentals = unique(fundamentals, by = c("symbol", "acceptedDate"))

# Merge features and fundamental data
features = fundamentals[features, on = c("symbol", "acceptedDate" = "date_ohlcv"), roll = Inf]
features[date == max(date), .(symbol, acceptedDate, acceptedDateTime, date, receivablesGrowth)]

# remove unnecesary columns
features[, `:=`(period = NULL, link = NULL, finalLink = NULL,
                reportedCurrency = NULL, cik = NULL, calendarYear = NULL)]

# Checks dates
features[symbol == symbol_, .(symbol, fundamental_date, acceptedDate, acceptedDateFundamentals, date)]

# convert char features to numeric features
char_cols = features[, colnames(.SD), .SDcols = is.character]
char_cols = setdiff(char_cols, c("symbol", "time", "right_time", "industry", "sector"))
features[, (char_cols) := lapply(.SD, as.numeric), .SDcols = char_cols]

############# ADD TRANSCRIPTS #################
# import transcripts sentiments datadata
# config <- tiledb_config()
# config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
# config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
# config["vfs.s3.region"] <- "us-east-1"
# context_with_config <- tiledb_ctx(config)
# arr <- tiledb_array("s3://equity-transcripts-sentiments",
#                     as.data.frame = TRUE,
#                     query_layout = "UNORDERED",
# )
# system.time(transcript_sentiments <- arr[])
# tiledb_array_close(arr)
# sentiments_dt <- as.data.table(transcript_sentiments)
# setnames(sentiments_dt, "date", "time_transcript")
# attr(sentiments_dt$time, "tz") <- "UTC"
# sentiments_dt[, date := as.Date(time)]
# sentiments_dt[, time := NULL]
# cols_sentiment = colnames(sentiments_dt)[grep("FLS", colnames(sentiments_dt))]

# merge with features
# features[, date_day_after_event_ := date_day_after_event]
# features <- sentiments_dt[features, on = c("symbol", "date" = "date_day_after_event_"), roll = Inf]
# features[, .(symbol, date, date_day_after_event, time_transcript, Not_FLS_positive)]
# features[1:50, .(symbol, date, date_day_after_event, time_transcript, Not_FLS_positive)]

# remove observations where transcripts are more than 2 days away
# features <- features[date - as.IDate(as.Date(time_transcript)) >= 3,
#                      (cols_sentiment) := NA]
# features[, ..cols_sentiment]
############# ADD TRANSCRIPTS ###############


# MACRO -------------------------------------------------------------------
# TODO ADD DAILY MACRO DATA AFTER I FINISH IMPORTING AND CLEANING MACRO DATA 
# # import FRED data
# fred_meta = fread(file.path("/home/sn/data/macro", "fred_meta.csv"))
# fred_meta[, unique(frequency_short)]
# fred_meta = fred_meta[frequency_short %chin% c("D", "W")]
# fred_meta = fred_meta[observation_start > as.IDate("2010-01-01") &
#                         observation_end > as.IDate("2023-12-31")]
# fred_dt = fread(file.path("/home/sn/data/macro", "fred_cleaned.csv"))
# fred_dt = fred_dt[series_id %chin% fred_meta[, id]]
# fred_dt = dcast(fred_dt, date_real ~ series_id, value.var = "value")
# dim(fred_dt)

# import macro factors
macros = read_parquet(
  path(
    "/home/sn/data/equity/us",
    "predictors_daily",
    "factors",
    "macro_factors",
    ext = "parquet"
  )
)

# Merge FRED and macro
macros = merge(macros, fred_dt, by.x = "date", by.y = "date_real", all.x = TRUE, all.y = FALSE)
macros = macros[date > as.IDate("2010-01-01")]

# Macro data
features[, date_features_ := date_features]
macros[, date_macro := date]
features = macros[features, on = c("date" = "date_features_"), roll = Inf]
features[, .(symbol, date, date_features, date_macro, vix)]

# Final checks for predictors
any(duplicated(features[, .(symbol, date_features)]))
features[duplicated(features[, .(symbol, date_features)]), .(symbol, date_features)]
features[duplicated(features[, .(symbol, date)]), .(symbol, date)]
features[, max(date)]
features[date == max(date), 1:25]


# FEATURES SPACE ----------------------------------------------------------
# Features space from features raw
cols_remove <- c("trading_date_after_event", "time", "datetime_investingcom",
                 "eps_investingcom", "eps_forecast_investingcom", "revenue_investingcom",
                 "revenue_forecast_investingcom", "time_dummy",
                 "trading_date_after_event", "fundamental_date", "cik", "link", "finalLink",
                 "fillingDate", "calendarYear", "eps.y", "revenue.y", "period.x", "period.y",
                 "acceptedDateTime", "acceptedDateFundamentals", "reportedCurrency",
                 "fundamental_acceptedDate", "period", "right_time",
                 "updatedFromDate", "fiscalDateEnding", "time_investingcom",
                 "same_announce_time", "eps", "epsEstimated", "revenue", "revenueEstimated",
                 "same_announce_time", "time_transcript", "i.time",
                 # remove dates we don't need
                 setdiff(colnames(features)[grep("date", colnames(features), ignore.case = TRUE)], c("date", "date_rolling")),
                 # remove columns with i - possible duplicates
                 colnames(features)[grep("i\\.|\\.y", colnames(features))],
                 colnames(features)[grep("^open\\.|^high\\.|^low\\.|^close\\.|^volume\\.|^returns\\.", 
                                         colnames(features))]
)
cols_non_features <- c("symbol", "date", "date_rolling", "time", "right_time", 
                       "open", "high", "low", "close", "volume", "returns",
                       target_variables)
cols_features = setdiff(colnames(features), c(cols_remove, cols_non_features))
head(cols_features, 10)
tail(cols_features, 500)
length(cols_features)
cols = c(cols_non_features, cols_features)
cols_remove = setdiff(cols, colnames(features))
cols = cols[!(cols %in% cols_remove)]
features = features[, .SD, .SDcols = cols]

# Checks
features[, max(date)]
# features[, .(symbol, date, date_rolling)]


# CLEAN DATA --------------------------------------------------------------
# Convert columns to numeric. This is important only if we import existing features
clf_data = copy(features)
chr_to_num_cols = setdiff(colnames(clf_data[, .SD, .SDcols = is.character]),
                          c("symbol", "time", "right_time", "industry", "sector"))
clf_data = clf_data[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
log_to_num_cols = colnames(clf_data[, .SD, .SDcols = is.logical])
clf_data = clf_data[, (log_to_num_cols) := lapply(.SD, as.numeric), .SDcols = log_to_num_cols]

# Remove duplicates
any_duplicates = any(duplicated(clf_data[, .(symbol, date)]))
if (any_duplicates) clf_data = unique(clf_data, by = c("symbol", "date"))

# Remove columns with many NA
keep_cols = names(which(colMeans(!is.na(clf_data)) > 0.6))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(clf_data), c(keep_cols, "right_time"))))
clf_data = clf_data[, .SD, .SDcols = keep_cols]

# Remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols = names(which(colMeans(!is.infinite(as.data.frame(clf_data))) > 0.98))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(clf_data), keep_cols)))
clf_data = clf_data[, .SD, .SDcols = keep_cols]

# Remove inf values
n_0 = nrow(clf_data)
clf_data = clf_data[is.finite(rowSums(clf_data[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
n_1 = nrow(clf_data)
print(paste0("Removing ", n_0 - n_1, " rows because of Inf values"))

# Final checks
# clf_data[, .(symbol, date, date_rolling)]
# features[, .(symbol, date, date_rolling)]
features[, max(date)]
clf_data[, max(date)]
clf_data[date == max(date), 1:15]
clf_data[date == max(date), 1500:1525]

# Save features
last_pead_date = strftime(clf_data[, max(date)], "%Y%m%d")
file_name = paste0("pre-predictors-", last_pead_date, ".csv")  
file_name_local = fs::path(PATH_PREDICTORS, file_name)
fwrite(clf_data, file_name_local)

# Add to padobran
# scp /home/sn/data/equity/us/predictors_daily/pead_predictors/pre-predictors-20241112.csv padobran:/home/jmaric/pread/pre-predictors.csv
