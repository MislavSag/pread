library(fs)
library(data.table)
library(mlr3verse)
library(mlr3batchmark)
library(batchtools)
library(duckdb)
library(PerformanceAnalytics)
library(AzureStor)
library(future.apply)
library(matrixStats)
library(finautoml)


# creds
blob_key = Sys.getenv("BLOBKEY")
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)

# Get data from padobran
dir_save = "experiment"
dir_path = path("/home/sn/data/strategies/pread/", dir_save)
if (!dir_exists(dir_path)) dir_create(dir_path)
# scp -r padobran:/home/jmaric/pread/experiments_pre/algorithms /home/sn/data/strategies/pread/experiment
# scp -r padobran:/home/jmaric/pread/experiments_pre/exports /home/sn/data/strategies/pread/experiment
# scp -r padobran:/home/jmaric/pread/experiments_pre/problems /home/sn/data/strategies/pread/experiment
# scp -r padobran:/home/jmaric/pread/experiments_pre/results /home/sn/data/strategies/pread/experiment
# scp -r padobran:/home/jmaric/pread/experiments_pre/updates /home/sn/data/strategies/pread/experiment
# scp padobran:/home/jmaric/pread/experiments_pre/registry.rds /home/sn/data/strategies/pread/experiment/registry.rds

# load registry
reg = loadRegistry(dir_path, work.dir=dir_path)

# used memory
reg$status[!is.na(mem.used)]
reg$status[, max(mem.used, na.rm = TRUE)]

# done jobs
results_files = fs::path_ext_remove(fs::path_file(dir_ls(fs::path(dir_path, "results"))))
ids_done = findDone(reg=reg)
ids_done = ids_done[job.id %in% results_files]
ids_notdone = findNotDone(reg=reg)

# Errors in logs
# 1)


# import already saved predictions
# fs::dir_ls("predictions")
# predictions = readRDS("predictions/predictions-20231025215620.rds")

# get results
tabs = batchtools::getJobTable(ids_done, reg = reg)[
  , c("job.id", "job.name", "repl", "prob.pars", "algo.pars"), with = FALSE]
predictions_meta = cbind.data.frame(
  id = tabs[, job.id],
  task = vapply(tabs$prob.pars, `[[`, character(1L), "task_id"),
  learner = gsub(".*regr.|.tuned", "", vapply(tabs$algo.pars, `[[`, character(1L), "learner_id")),
  cv = gsub("custom_|_.*", "", vapply(tabs$prob.pars, `[[`, character(1L), "resampling_id")),
  fold = gsub("custom_\\d+_", "", vapply(tabs$prob.pars, `[[`, character(1L), "resampling_id"))
)
predictions_l = lapply(unlist(ids_done), function(id_) {
  # id_ = 10035
  x = tryCatch({readRDS(fs::path(dir_path, "results", id_, ext = "rds"))},
               error = function(e) NULL)
  if (is.null(x)) {
    print(id_)
    return(NULL)
  }
  x["id"] = id_
  x
})
predictions = lapply(predictions_l, function(x) {
  cbind.data.frame(
    id = x$id,
    row_ids = x$prediction$test$row_ids,
    truth = x$prediction$test$truth,
    response = x$prediction$test$response
  )
})
predictions = rbindlist(predictions)
predictions = merge(predictions_meta, predictions, by = "id")
predictions = as.data.table(predictions)

# import tasks
tasks_files = dir_ls(fs::path(dir_path, "problems"))
tasks = lapply(tasks_files, readRDS)
names(tasks) = lapply(tasks, function(t) t$data$id)
tasks

# backends
id_cols = c("symbol", "date", "..row_id", "target")
backend = tasks[[1]]$data$backend
backend$data(rows = backend$rownames, cols = id_cols)
backend = backend$data(rows = backend$rownames, cols = id_cols)
setnames(backend, "..row_id", "row_ids")

# measures
mlr_measures$add("linex", finautoml::Linex)
mlr_measures$add("adjloss2", finautoml::AdjLoss2)

# merge backs and predictions
predictions = backend[predictions, on = c("row_ids")]
predictions[, date := as.Date(date)]
setnames(predictions,
         c("task_names", "learner_names", "cv_names"),
         c("task", "learner", "cv"),
         skip_absent = TRUE)


# PREDICTIONS RESULTS -----------------------------------------------------
# remove dupliactes - keep firt
predictions = unique(predictions, by = c("row_ids", "date", "task", "learner", "cv"))

# predictions
sign01 = function(x) {
  ifelse(x > 0, 1, 0)
}
predictions[, `:=`(
  truth_sign = as.factor(sign01(truth)),
  response_sign = as.factor(sign01(response))
)]

# Remove na value
predictions_dt = na.omit(predictions)

# number of predictions by task and cv
unique(predictions_dt, by = c("task", "learner", "cv", "row_ids"))[, .N, by = c("task")]
unique(predictions_dt, by = c("task", "learner", "cv", "row_ids"))[, .N, by = c("task", "cv")]

# Classification measures across ids
measures = function(t, res) {
  list(acc   = mlr3measures::acc(t, res),
       fbeta = mlr3measures::fbeta(t, res, positive = "1"),
       tpr   = mlr3measures::tpr(t, res, positive = "1"),
       tnr   = mlr3measures::tnr(t, res, positive = "1"))
}
predictions_dt[, measures(truth_sign, response_sign), by = c("cv")]
predictions_dt[, measures(truth_sign, response_sign), by = c("task")]
predictions_dt[, measures(truth_sign, response_sign), by = c("learner")]
predictions_dt[, measures(truth_sign, response_sign), by = c("cv", "task")]
predictions_dt[, measures(truth_sign, response_sign), by = c("cv", "learner")]
# predictions[, measures(truth_sign, response_sign), by = c("cv", "task", "learner")][order(V1)]

# create truth factor
predictions_dt[, truth_sign := as.factor(sign(truth))]

# prediction to wide format
predictions_wide = dcast(
  predictions_dt,
  task + symbol + date + truth + truth_sign + target ~ learner,
  value.var = "response"
)

# ensambles
cols = colnames(predictions_wide)
cols = cols[which(cols == "glmnet"):ncol(predictions_wide)]
p = predictions_wide[, ..cols]
pm = as.matrix(p)
predictions_wide = cbind(predictions_wide, mean_resp = rowMeans(p, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, median_resp = rowMedians(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, sum_resp = rowSums2(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, iqrs_resp = rowIQRs(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, sd_resp = rowMads(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, q9_resp = rowQuantiles(pm, probs = 0.9, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, max_resp = rowMaxs(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, min_resp = rowMins(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, all_buy = rowAlls(pm >= 0, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, all_sell = rowAlls(pm < 0, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, sum_buy = rowSums2(pm >= 0, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, sum_sell = rowSums2(pm < 0, na.rm = TRUE))
predictions_wide = na.omit(predictions_wide)

# results by ensamble statistics for classification measures
calculate_measures = function(t, res) {
  is_na_ = which(is.na(t))
  if (length(is_na_) > 0) {
    res = res[-is_na_]
    t = t[-is_na_]
  }
  list(
    acc       = mlr3measures::acc(t, res),
    fbeta     = mlr3measures::fbeta(t, res, positive = "1"),
    tpr       = mlr3measures::tpr(t, res, positive = "1"),
    precision = mlr3measures::precision(t, res, positive = "1"),
    tnr       = mlr3measures::tnr(t, res, positive = "1"),
    npv       = mlr3measures::npv(t, res, positive = "1")
  )
}
cols_ens = c("task", "symbol", "date", "truth", "target", "mean_resp", "truth_sign",
             "median_resp", "sum_resp", "q9_resp", "max_resp", "min_resp")
predictions_wide_ens = predictions_wide[, ..cols_ens]
predictions_wide_ens = na.omit(predictions_wide_ens)
predictions_wide_ens[, truth_sign := as.factor(sign01(as.integer(as.character(truth_sign))))]
# predictions_wide_ens[, truth := as.factor(sign01(as.integer(as.character(truth))))]

predictions_wide_ens[, calculate_measures(truth_sign, as.factor(sign01(mean_resp))), by = task]
predictions_wide_ens[, calculate_measures(truth_sign, as.factor(sign01(median_resp))), by = task]
predictions_wide_ens[, calculate_measures(truth_sign, as.factor(sign01(sum_resp))), by = task]
predictions_wide_ens[, calculate_measures(truth_sign, as.factor(sign01(max_resp + min_resp))), by = task]
predictions_wide_ens[, calculate_measures(truth_sign, as.factor(sign01(q9_resp))), by = task]

# Performance by returns
cols = colnames(predictions_wide)
cols = cols[which(cols == "earth"):which(cols == "sum_resp")]
cols = c("task", cols)
melt(na.omit(predictions_wide[, ..cols]), id.vars = "task")[value > 0, sum(value),
                                                            by = .(task, variable)][order(V1)]
melt(na.omit(predictions_wide[, ..cols]), id.vars = "task")[value > 0 & value < 2, sum(value),
                                                            by = .(task, variable)][order(V1)]

# Save to azure for QC backtest
cont = storage_container(BLOBENDPOINT, "qc-backtest")
file_name_ =  paste0("pre_qc.csv")
qc_data = unique(predictions_wide, by = c("task", "symbol", "date"))
# qc_data = na.omit(qc_data)
setorder(qc_data, task, date)
qc_data[, .(min_date = min(date), max_date = max(date))]
last(qc_data[, .(task, symbol, date, truth, target, ranger)], 100)
storage_write_csv(qc_data, cont, file_name_)


# SYSTEMIC RISK -----------------------------------------------------------
# import SPY data
con <- dbConnect(duckdb::duckdb())
query <- sprintf("
    SELECT *
    FROM '/home/sn/lean/data/stocks_daily.csv'
    WHERE Symbol = 'spy'
")
spy <- dbGetQuery(con, query)
dbDisconnect(con)
spy = as.data.table(spy)
spy = spy[, .(date = Date, close = `Adj Close`)]
spy[, returns := close / shift(close) - 1]
spy = na.omit(spy)
plot(spy[, close])

# systemic risk
indicator = predictions_wide[, .(
  indicator = mean(mean_resp, na.rm = TRUE),
  indicator_sd = sd(mean_resp, na.rm = TRUE),
  indicator_q1 = quantile(mean_resp, probs = 0.01, na.rm = TRUE)
),
by = date][order(date)]
cols = colnames(indicator)[2:ncol(indicator)]
indicator[, (cols) := lapply(.SD, nafill, type = "locf"), .SDcols = cols]
indicator[, `:=`(
  indicator_ema = TTR::EMA(indicator, 5, na.rm = TRUE),
  indicator_sd_ema = TTR::EMA(indicator_sd, 5, na.rm = TRUE),
  indicator_q1_ema = TTR::EMA(indicator_q1, 5, na.rm = TRUE)
)]
indicator = na.omit(indicator)
plot(as.xts.data.table(indicator)[, 4])
plot(as.xts.data.table(indicator)[, 5])
plot(as.xts.data.table(indicator)[, 6])

# create backtest data
backtest_data =  merge(spy, indicator, by = "date", all.x = TRUE, all.y = FALSE)
min_date = indicator[, min(date)]
backtest_data = backtest_data[date > min_date]
max_date = indicator[, max(date)]
backtest_data = backtest_data[date < max_date]
cols = colnames(backtest_data)[4:ncol(backtest_data)]
backtest_data[, (cols) := lapply(.SD, nafill, type = "locf"), .SDcols = cols]
backtest_data[, signal := 1]
backtest_data[shift(indicator_ema) < 0, signal := 0]
# backtest_data[shift(indicator_sd_ema) < 4, signal := 0]
backtest_data_xts = as.xts.data.table(backtest_data[, .(date, benchmark = returns, strategy = ifelse(signal == 0, 0, returns * signal * 1))])
charts.PerformanceSummary(backtest_data_xts)
# backtest performance
Performance <- function(x) {
  cumRetx = Return.cumulative(x)
  annRetx = Return.annualized(x, scale=252)
  sharpex = SharpeRatio.annualized(x, scale=252)
  winpctx = length(x[x > 0])/length(x[x != 0])
  annSDx = sd.annualized(x, scale=252)
  
  DDs <- findDrawdowns(x)
  maxDDx = min(DDs$return)
  # maxLx = max(DDs$length)
  
  Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx) # , maxLx)
  names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
                  "Win %", "Annualized Volatility", "Maximum Drawdown") # "Max Length Drawdown")
  return(Perf)
}
Performance(backtest_data_xts[, 1])
Performance(backtest_data_xts[, 2])

# analyse indicator
library(forecast)
ndiffs(as.xts.data.table(indicator)[, 1])
plot(diff(as.xts.data.table(indicator)[, 1]))
