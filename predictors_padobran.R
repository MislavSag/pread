options(progress_enabled = FALSE)

library(data.table)
library(finfeatures)
library(reticulate)


# python environment
# reticulate::use_virtualenv("C:/Users/Mislav/projects_py/pyquant", required = TRUE)
# theftms::init_theft("/opt/venv")
reticulate::use_virtualenv("/opt/venv")
tsfel = reticulate::import("tsfel")
tsfresh = reticulate::import("tsfresh", convert = FALSE)
warnigns = reticulate::import("warnings", convert = FALSE)
warnigns$filterwarnings('ignore')

# paths
PATH_PREDICTORS = file.path("./predictors_padobran")

# Create directory if it doesnt exists
if (!dir.exists(PATH_PREDICTORS)) {
  dir.create(PATH_PREDICTORS)
}

# Get index
i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
# i = 1

# Get data
dataset = fread("dataset_pread.csv")
ohlcv = fread("prices_pread.csv")
# dataset = fread("/home/sn/data/strategies/pread/dataset_pread.csv") # DEBUG
# ohlcv = fread("/home/sn/data/strategies/pread/prices_pread.csv") # DEBUG

# Create Ohlcv object
ohlcv = Ohlcv$new(ohlcv[, .(symbol, date, open, high, low, close, volume)], 
                  date_col = "date")
  
# Lag parameter
# ako je red u events amc. label je open_t+1 / close_t; lag je 1L
# ako je red u events bmo. label je open_t / close_t-1; lag je 2L
# radi jednostavnosti lag_ = 2L
lag_ = 2L

# Window
# Beaware of data size
workers = 2L

# Default windows. Set widnows you use the most in rolling predictors
windows = c(66, 252) # day and 2H;  cca 10 days

# Define at parameter
at_ = merge(ohlcv$X[, .(symbol, date)],
            dataset[, .(symbol, date, index = TRUE)],
            by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
at_ = at_[, which(index == TRUE)]

# Help function to save rolling predictors output
create_path = function(name) {
  file.path(PATH_PREDICTORS, paste0(name, "-", i, ".csv"))
}

# Divide data on 1000 rows
split_vector_into_chunks <- function(vector, chunks = 10000) {
  n <- length(vector)
  
  # Number of elements in most chunks
  base_size <- floor(n / chunks)
  
  # Calculate how many chunks need one extra element to account for the remainder
  remainder <- n %% chunks
  extra_sizes <- ifelse(seq_len(chunks) <= remainder, 1, 0)
  
  # Total elements per chunk
  chunk_sizes <- base_size + extra_sizes
  
  # Create the breaks for the chunks
  breaks <- c(0, cumsum(chunk_sizes))
  
  # Split the vector into chunks
  subsamples <- vector("list", length = chunks)
  for (i in seq_len(chunks)) {
    subsamples[[i]] <- vector[length = chunk_sizes[i]]
    subsamples[[i]] <- vector[(breaks[i] + 1):breaks[i + 1]]
  }
  
  return(subsamples)
}
at_sets = split_vector_into_chunks(at_, chunks = 10000)
at = at_sets[[i]]

# Exuber
path_ =   create_path("exuber")
windows_ = c(windows, 504)
if (max(at) > min(windows)) {
  exuber_init = RollingExuber$new(
    windows = windows_,
    workers = workers,
    at = at,
    lag = lag_,
    exuber_lag = c(1L)
  )
  exuber = exuber_init$get_rolling_features(ohlcv, log_prices = TRUE)
  fwrite(exuber, path_)
}

# Backcusum
path_ = create_path("backcusum")
if (max(at) > min(windows)) {
  backcusum_init = RollingBackcusum$new(
    windows = windows_,
    workers = workers,
    at = at,
    lag = lag_,
    alternative = c("greater", "two.sided"),
    return_power = c(1, 2))
  backcusum = backcusum_init$get_rolling_features(ohlcv)
  fwrite(backcusum, path_) 
}

# Forecasts
path_ = create_path("forecasts")
windows_ = c(252, 252 * 2)
if (max(at) > min(windows)) {
  forecasts_init = RollingForecats$new(
    windows = windows_,
    workers = workers,
    at = at,
    lag = lag_,
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
    lag = lag_,
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
    lag = lag_,
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
    lag = lag_,
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
    lag = lag_,
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
    lag = lag_,
    nar = c(1), 
    nma = c(1),
    bandw_exp = c(0.1, 0.5, 0.9))
  fracdiff = suppressMessages(fracdiff_init$get_rolling_features(ohlcv))
  fwrite(fracdiff, path_)
}

# Info
sprintf("There should be max %d files in PATH_PREDICTORS", 8 * 1000)
