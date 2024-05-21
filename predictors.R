library(data.table)
library(fs)
library(finfeatures)


# SETUP -------------------------------------------------------------------
# Paths
PATH                  = "/home/sn/data/strategies/pread"
PATH_ROLLING          = path(PATH, "predictors")
PATH_ROLLING_PADOBRAN = path(PATH, "predictors_padobran")

# PRICES AND EVENTS -------------------------------------------------------
# Get data
dataset = fread(path(PATH, "dataset_pread.csv"))
ohlcv   = fread(path(PATH, "prices_pread.csv"))
ohlcv = Ohlcv$new(ohlcv[, .(symbol, date, open, high, low, close, volume)], 
                  date_col = "date")

# ROLLING PREDICTORS ------------------------------------------------------
# Command to get data from padobran. This is necessary only first time
# scp -r padobran:/home/jmaric/pread/ /home/sn/data/strategies/pread/predictors/

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
lag_ = 2L
workers = 2L
windows = c(66, 252) # day and 2H;  cca 10 days

# Define at parameter
ohlcv_max_date = ohlcv$X[, max(date)]
get_at = function(n = "exuber.csv") {
  # n = "backcusum.csv"
  predictors_ = fread(path(PATH_ROLLING, n))
  new_dataset = fsetdiff(dataset[, .(symbol, date = as.IDate(date))],
                         predictors_[, .(symbol, date)])
  new_dataset[, date_merge := fifelse(date > ohlcv_max_date, ohlcv_max_date, date)]
  new_data = merge(
    ohlcv$X[, .(symbol, date, date_ohlcv = date)],
    new_dataset[, .(symbol, date_event = date, date = date_merge, index = TRUE)],
    by = c("symbol", "date"),
    all.x = TRUE,
    all.y = FALSE
  )
  # new_data[index == TRUE] # debug
  new_data[, which(index == TRUE)]
}

# Exuber
n_ = "exuber.csv"
at_ = get_at(n_)
windows_ = c(windows, 504)
exuber_init = RollingExuber$new(
  windows = windows_,
  workers = workers,
  at = at_,
  lag = 0L,
  exuber_lag = 1L
)
exuber = exuber_init$get_rolling_features(ohlcv, TRUE)
fwrite(exuber, path_)

# Backcusum
n_ = "backcusum.csv"
at_ = get_at(n_)
windows_ = c(windows, 504)
backcusum_init = RollingBackcusum$new(
  windows = windows_,
  workers = workers,
  at = at_,
  lag = 0L,
  alternative = c("greater", "two.sided"),
  return_power = c(1, 2)
)
backcusum = backcusum_init$get_rolling_features(ohlcv)
fwrite(backcusum, path_)

# Forecasts
path_ = create_path("forecasts")
windows_ = c(252, 252 * 2)
if (max(at) > min(windows)) {
  forecasts_init = RollingForecats$new(
    windows = windows_,
    workers = workers,
    at = at,
    lag = 0L,
    forecast_type = c("autoarima", "nnetar", "ets"),
    h = 22)
  forecasts = suppressMessages(forecasts_init$get_rolling_features(ohlcv))
  fwrite(forecasts, path_)
}

# Theft r
path_ = create_path("theftr")
windows_ = c(5, 22, windows)
if (max(at) > min(windows)) {
  theft_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = at,
    lag = 0L,
    features_set = c("catch22", "feasts"))
  theft_r = theft_init$get_rolling_features(ohlcv)
  fwrite(theft_r, path_)
}

# Theft py
path_ = create_path("theftpy")
windows_ = c(22, windows)
if (max(at) > min(windows)) {
  theft_init = RollingTheft$new(
    windows = windows_,
    workers = 1L,
    at = at,
    lag = 0L,
    features_set = c("tsfel", "tsfresh"))
  theft_py = suppressMessages(theft_init$get_rolling_features(ohlcv))
  fwrite(theft_py, path_)
}

# Tsfeatures
path_ = create_path("tsfeatures")
if (max(at) > min(windows)) {
  tsfeatures_init = RollingTsfeatures$new(
    windows = windows,
    workers = workers,
    at = at,
    lag = 0L,
    scale = TRUE)
  tsfeatures = suppressMessages(tsfeatures_init$get_rolling_features(ohlcv))
  fwrite(tsfeatures, path_)
}

# WaveletArima
path_ = create_path("waveletarima")
if (max(at) > min(windows)) {
  waveletarima_init = RollingWaveletArima$new(
    windows = windows,
    workers = workers,
    at = at,
    lag = 0L,
    filter = "haar")
  waveletarima = suppressMessages(waveletarima_init$get_rolling_features(ohlcv))
  fwrite(waveletarima, path_)
}

# FracDiff
path_ = create_path("fracdiff")
if (max(at) > min(windows)) {
  fracdiff_init = RollingFracdiff$new(
    windows = windows,
    workers = workers,
    at = at,
    lag = 0L,
    nar = c(1), 
    nma = c(1),
    bandw_exp = c(0.1, 0.5, 0.9))
  fracdiff = suppressMessages(fracdiff_init$get_rolling_features(ohlcv))
  fwrite(fracdiff, path_)
}

# OHLCV PREDICTORS --------------------------------------------------------


