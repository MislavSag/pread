# remotes::install_github("MislavSag/finautoml")
library(AzureStor)
library(data.table)
library(fs)
library(lubridate)
library(paradox)
library(mlr3)
library(mlr3pipelines)
library(mlr3filters)
library(mlr3tuning)
library(mlr3extralearners)
library(finautoml)
library(glue)
library(batchtools)
library(mlr3batchmark)
library(future)
# library(mlr3torch)


# SETUP -------------------------------------------------------------------
# utils https://stackoverflow.com/questions/1995933/number-of-months-between-two-dates
monnb <- function(d) {
  lt <- as.POSIXlt(as.Date(d, origin="1900-01-01"))
  lt$year*12 + lt$mon }
mondf <- function(d1, d2) { monnb(d2) - monnb(d1) }
diff_in_weeks = function(d1, d2) difftime(d2, d1, units = "weeks") # weeks

# snake to camel
snakeToCamel <- function(snake_str) {
  # Replace underscores with spaces
  spaced_str <- gsub("_", " ", snake_str)
  
  # Convert to title case using tools::toTitleCase
  title_case_str <- tools::toTitleCase(spaced_str)
  
  # Remove spaces and make the first character lowercase
  camel_case_str <- gsub(" ", "", title_case_str)
  camel_case_str <- sub("^.", tolower(substr(camel_case_str, 1, 1)), camel_case_str)
  
  # I haeve added this to remove dot
  camel_case_str <- gsub("\\.", "", camel_case_str)
  
  return(camel_case_str)
}

# Azure creditentials
endpoint = "https://snpmarketdata.blob.core.windows.net/"
key = Sys.getenv("BLOBKEY")
BLOBENDPOINT = storage_endpoint(endpoint, key=key)
cont = storage_container(BLOBENDPOINT, "qc-live")

# Check if today is sutarday
MODEL = FALSE
if (weekdays(Sys.Date()) == "sabato") {
  print("Today is Saturday, modeling time.")
  MODEL = TRUE
}

# Paths
SAVEPATH = "/home/sn/data/models/pread/predictions"


# PREPARE DATA ------------------------------------------------------------
print("Prepare data")

# Read predictors
pead_file_local = list.files(
  "/home/sn/data/equity/us/predictors_daily/pead_predictors",
  pattern = "pre-",
  full.names = TRUE
)
dates = as.Date(gsub(".*-", "", path_ext_remove(path_file(pead_file_local))),
                format = "%Y%m%d")
DT = fread(pead_file_local[which.max(dates)])

# Remove industry and sector vars
DT[, `:=`(industry = NULL, sector = NULL, right_time = NULL)]

# Define target variable
DT[, target := amc_return]
DT[time == "bmo", target := bmo_return]
DT[, .(symbol, date, time, amc_return, bmo_return, target)]
DT[, `:=`(amc_return = NULL, bmo_return = NULL)]

# Define target variable for live observations
cols_ = colnames(DT)[c(1:10, ncol(DT))]
DT[, ..cols_]
DT[is.na(target), ..cols_]
DT[is.na(target) & date > (Sys.Date() - 6), ..cols_]
DT[is.na(target) & date > (Sys.Date() - 6), target := 0]
DT[date == max(date, na.rm = TRUE), ..cols_]

# define predictors
cols_non_features = c(
  "symbol", "date", "time", "open", "high", "low", "close", "volume", "returns", "date_rolling"
)
cols_features = setdiff(colnames(DT), c(cols_non_features, "target"))

# change feature and targets columns names due to lighgbm
cols_features_new = vapply(cols_features, snakeToCamel, FUN.VALUE = character(1L), USE.NAMES = FALSE)
setnames(DT, cols_features, cols_features_new)
cols_features = cols_features_new

# Convert columns to numeric. This is important only if we import existing features
chr_to_num_cols = setdiff(colnames(DT[, .SD, .SDcols = is.character]), c("symbol", "time"))
print(chr_to_num_cols)
DT = DT[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]

# Remove constant columns in set
features_ = DT[, ..cols_features]
remove_cols = colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
cols_features = setdiff(cols_features, remove_cols)

# Convert variables with low number of unique values to factors
int_numbers = na.omit(DT[, ..cols_features])[, lapply(.SD, function(x) all(floor(x) == x))]
int_cols = colnames(DT[, ..cols_features])[as.matrix(int_numbers)[1,]]
factor_cols = DT[, ..int_cols][, lapply(.SD, function(x) length(unique(x)))]
factor_cols = as.matrix(factor_cols)[1, ]
factor_cols = factor_cols[factor_cols <= 100]
DT = DT[, (names(factor_cols)) := lapply(.SD, as.factor), .SD = names(factor_cols)]

# remove observations with missing target
# if we want to keep as much data as possible an use only one predict horizont
# we can skeep this step
DT = na.omit(DT, cols = "target")

# filter data after 2013-07-01
DT = DT[date >= as.IDate("2015-02-01")]

# change IDate to date, because of error
# Assertion on 'feature types' failed: Must be a subset of
# {'logical','integer','numeric','character','factor','ordered','POSIXct'},
# but has additional elements {'IDate'}.
DT[, date := as.POSIXct(date, tz = "UTC")]

# Sort
# this returns error on HPC. Some problem with memory
# setorder(DT, date)
print("This was the problem")
DT = DT[order(date)]
head(DT[, .(symbol, date, target)], 15)
print("This was the problem. Solved.")

# Checks
DT[, max(date)]
DT[date == max(date)][, 1:10]
DT[date == max(date)][, colnames(.SD)[colSums(is.na(.SD)) == .N]]
DT[date == max(date), .(iHLIDX32820)]
DT[date == max(date)][, colnames(.SD)[colSums(is.na(.SD)) > 0]]

# Remove columns with missing values for last observations
if (MODEL) {
  cols_remove = DT[date == max(date)][, colnames(.SD)[colSums(is.na(.SD)) > 0]]
  DT = DT[, .SD, .SDcols = -cols_remove]
  cols_features = setdiff(cols_features, cols_remove)
}


# TASKS -------------------------------------------------------------------
# ID columns we always keep
id_cols = c("symbol", "date", "target")

# Create task
cols_ = c(id_cols, cols_features)
task = as_task_regr(DT[, ..cols_], id = "pre", target = "target")

# Set roles for id columns
task$col_roles$feature = setdiff(task$col_roles$feature, id_cols)


# CROSS VALIDATIONS -------------------------------------------------------
create_custom_rolling_windows = function(task,
                                         duration_unit = "month",
                                         train_duration,
                                         gap_duration,
                                         tune_duration,
                                         test_duration) {
  # Function to convert durations to the appropriate period
  convert_duration <- function(duration) {
    if (duration_unit == "week") {
      weeks(duration)
    } else { # defaults to months if not weeks
      months(duration)
    }
  }
  
  # Define row ids
  data = task$backend$data(cols = c("date", "..row_id"),
                           rows = 1:task$nrow)
  setnames(data, "..row_id", "row_id")
  stopifnot(all(task$row_ids == data[, row_id]))
  
  # Ensure date is in Date format
  data[, date_col := as.Date(date)]
  
  # Initialize start and end dates based on duration unit
  start_date <- if (duration_unit == "week") {
    floor_date(min(data$date_col), "week")
  } else {
    floor_date(min(data$date_col), "month")
  }
  end_date <- if (duration_unit == "week") {
    ceiling_date(max(data$date_col), "week") - days(1)
  } else {
    ceiling_date(max(data$date_col), "month") - days(1)
  }
  
  # Initialize folds list
  folds <- list()
  
  while (start_date < end_date) {
    train_end = start_date %m+% convert_duration(train_duration) - days(1)
    gap1_end  = train_end %m+% convert_duration(gap_duration)
    tune_end  = gap1_end %m+% convert_duration(tune_duration)
    gap2_end  = tune_end %m+% convert_duration(gap_duration)
    test_end  = gap2_end %m+% convert_duration(test_duration) - days(1)
    
    # Ensure the fold does not exceed the data range
    if (test_end > end_date) {
      break
    }
    
    # Extracting indices for each set
    train_indices = data[date_col %between% c(start_date, train_end), row_id]
    tune_indices  = data[date_col %between% c(gap1_end + days(1), tune_end), row_id]
    test_indices  = data[date_col %between% c(gap2_end + days(1), test_end), row_id]
    
    folds[[length(folds) + 1]] <- list(train = train_indices, tune = tune_indices, test = test_indices)
    
    # Update the start date for the next fold
    start_date = if (duration_unit == "week") {
      start_date %m+% weeks(1)
    } else {
      start_date %m+% months(1)
    }
  }
  
  # Prepare sets for inner and outer resampling
  train_sets = lapply(folds, function(fold) fold$train)
  tune_sets  = lapply(folds, function(fold) fold$tune)
  test_sets  = lapply(folds, function(fold) fold$test)
  
  # Combine train and tune sets for outer resampling
  inner_sets = lapply(seq_along(train_sets), function(i) {
    c(train_sets[[i]], tune_sets[[i]])
  })
  
  # Instantiate custom inner resampling (train: train, test: tune)
  custom_inner = rsmp("custom", id = paste0(task$id, "-inner"))
  custom_inner$instantiate(task, train_sets, tune_sets)
  
  # Instantiate custom outer resampling (train: train+tune, test: test)
  custom_outer = rsmp("custom", id = paste0(task$id, "-outer"))
  custom_outer$instantiate(task, inner_sets, test_sets)
  
  return(list(outer = custom_outer, inner = custom_inner))
}

# create list of cvs
custom_cvs = list()
custom_cvs[[1]] = create_custom_rolling_windows(
  task = task$clone(),
  duration_unit = "month",
  train_duration = 48,
  gap_duration = 1,
  tune_duration = 6,
  test_duration = 2
)


# # PREDICTIONS -------------------------------------------------------------
# if (!MODEL) {
#   # get data for last fold
#   cv_live = task$backend$data(
#     rows = custom_cvs[[1]]$outer$instance$test[[custom_cvs[[1]]$outer$iters]],
#     cols = task$backend$colnames[1:15]
#   )
#   cv_live[date == Sys.Date()]
#   
#   # get last registry folder
#   live_dirs = dir_ls(regexp = "experiments_pread_live")
#   dates_ = as.Date(gsub(".*?(\\d+)", "\\1", live_dirs), format = "%Y%m%d")
#   last_file = names(dates_[which.max(dates_)])
#   
#   # read registriy
#   reg = loadRegistry(last_file)
#   
#   # read results
#   results = reduceResultsBatchmark(reg = reg)
#   
#   # predictions for today
#   results$
# }


# ADD PIPELINES -----------------------------------------------------------
print("Add pipelines")

# source pipes, filters and other
# source("mlr3_gausscov_f3st.R")
# source("PipeOpFilterRegrTarget.R")
# measures
# source("PortfolioRet.R")

# add my pipes to mlr dictionary
mlr_pipeops$add("uniformization", finautoml::PipeOpUniform)
# mlr_pipeops$add("winsorize", PipeOpWinsorize)
mlr_pipeops$add("winsorizesimple", finautoml::PipeOpWinsorizeSimple)
# mlr_pipeops$add("winsorizesimplegroup", PipeOpWinsorizeSimpleGroup)
mlr_pipeops$add("dropna", finautoml::PipeOpDropNA)
mlr_pipeops$add("dropnacol", finautoml::PipeOpDropNACol)
mlr_pipeops$add("dropcorr", finautoml::PipeOpDropCorr)
# mlr_pipeops$add("pca_explained", PipeOpPCAExplained)
mlr_pipeops$add("filter_target", finautoml::PipeOpFilterRegrTarget)
mlr_filters$add("gausscov_f1st", finautoml::FilterGausscovF1st)
# mlr_filters$add("gausscov_f3st", FilterGausscovF3st)
mlr_measures$add("linex", finautoml::Linex)
mlr_measures$add("adjloss2", finautoml::AdjLoss2)
# mlr_measures$add("portfolio_ret", PortfolioRet)


# LEARNERS ----------------------------------------------------------------
# graph templates
gr = gunion(list(
  po("nop", id = "nop_union_pca"),
  po("pca", center = FALSE, rank. = 50),
  po("ica", n.comp = 10)
  # po("ica", n.comp = 10) # Error in fastICA::fastICA(as.matrix(dt), n.comp = 10L, method = "C") : data must be matrix-conformal This happened PipeOp ica's $train()
)) %>>% po("featureunion", id = "feature_union_pca")
filter_target_gr = po("branch",
                      options = c("nop_filter_target", "filter_target_select"),
                      id = "filter_target_branch") %>>%
  gunion(list(
    po("nop", id = "nop_filter_target"),
    po("filter_target", id = "filter_target_id")
  )) %>>%
  po("unbranch", id = "filter_target_unbranch")
# create mlr3 graph that takes 3 filters and union predictors from them
filters_ = list(
  po("filter", flt("disr"), filter.nfeat = 5),
  po("filter", flt("jmim"), filter.nfeat = 5),
  po("filter", flt("jmi"), filter.nfeat = 5),
  po("filter", flt("mim"), filter.nfeat = 5),
  po("filter", flt("mrmr"), filter.nfeat = 5),
  po("filter", flt("njmim"), filter.nfeat = 5),
  po("filter", flt("cmim"), filter.nfeat = 5),
  po("filter", flt("carscore"), filter.nfeat = 5), # UNCOMMENT LATER< SLOWER SO COMMENTED FOR DEVELOPING
  po("filter", flt("information_gain"), filter.nfeat = 5),
  po("filter", filter = flt("relief"), filter.nfeat = 5),
  po("filter", filter = flt("gausscov_f1st"), p0 = 0.25, filter.cutoff = 0)
  # po("filter", mlr3filters::flt("importance", learner = mlr3::lrn("classif.rpart")), filter.nfeat = 10, id = "importance_1"),
  # po("filter", mlr3filters::flt("importance", learner = lrn), filter.nfeat = 10, id = "importance_2")
)
graph_filters = gunion(filters_) %>>%
  po("featureunion", length(filters_), id = "feature_union_filters")

graph_template =
  filter_target_gr %>>%
  po("subsample") %>>% # uncomment this for hyperparameter tuning
  po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  # po("winsorizesimplegroup", group_var = "weekid", id = "winsorizesimplegroup", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  # po("uniformization") %>>%
  # scale branch
  po("branch", options = c("uniformization", "scale"), id = "scale_branch") %>>%
  gunion(list(po("uniformization"),
              po("scale")
  )) %>>%
  po("unbranch", id = "scale_unbranch") %>>%
  po("dropna", id = "dropna_v2") %>>%
  # add pca columns
  gr %>>%
  # filters
  # OLD BARNCH WAY
  # po("branch", options = c("jmi", "gausscovf1", "gausscovf3"), id = "filter_branch") %>>%
  # gunion(list(po("filter", filter = flt("jmi"), filter.nfeat = 25),
  #             # po("filter", filter = flt("relief"), filter.nfeat = 25),
  #             po("filter", filter = flt("gausscov_f1st"), p0 = 0.2, kmn=10, filter.cutoff = 0),
  #             po("filter", filter = flt("gausscov_f3st"), p0 = 0.2, kmn=10, m = 1, filter.cutoff = 0)
  # )) %>>%
  # po("unbranch", id = "filter_unbranch") %>>%
  # OLD BARNCH WAY
  # filters union
  graph_filters %>>%
  # modelmatrix
  # po("branch", options = c("nop_interaction", "modelmatrix"), id = "interaction_branch") %>>%
  # gunion(list(
  #   po("nop", id = "nop_interaction"),
  #   po("modelmatrix", formula = ~ . ^ 2))) %>>%
  # po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0)

# hyperparameters template
as.data.table(graph_template$param_set)[1:100]
gausscov_sp = as.character(seq(0.2, 0.8, by = 0.1))
winsorize_sp =  c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)
search_space_template = ps(
  # filter target
  filter_target_branch.selection = p_fct(levels = c("nop_filter_target", "filter_target_select")),
  filter_target_id.q = p_fct(levels = c("0.4", "0.3", "0.2"),
                             trafo = function(x, param_set) return(as.double(x)),
                             depends = filter_target_branch.selection == "filter_target_select"),
  # subsample for hyperband
  subsample.frac = p_dbl(0.7, 1, tags = "budget"), # commencement this if we want to use hyperband optimization
  # preprocessing
  dropcorr.cutoff = p_fct(c("0.80", "0.90", "0.95", "0.99"),
                          trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_high = p_fct(as.character(winsorize_sp),
                                     trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_low = p_fct(as.character(1-winsorize_sp),
                                    trafo = function(x, param_set) return(as.double(x))),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale"))
)

# if (interactive()) {
#   # show all combinations from search space, like in grid
#   sp_grid = generate_design_grid(search_space_template, 1)
#   sp_grid = sp_grid$data
#   sp_grid
#
#   # check ids of nth cv sets
#   train_ids = custom_cvs[[1]]$inner$instance$train[[1]]
#
#   # help graph for testing preprocessing
#   preprocess_test = function(
    #     fb_ = c("nop_filter_target", "filter_target_select")
#     ) {
#     fb_ = match.arg(fb_) # fb_ = "nop_filter_target"
#     task_ = tasks[[1]]$clone()
#     nr = task_$nrow
#     rows_ = (nr-10000):nr
#     # task_$filter(rows_)
#     task_$filter(train_ids)
#     # dates = task_$backend$data(rows_, "date")
#     # print(dates[, min(date)])
#     # print(dates[, max(date)])
#     gr_test = graph_template$clone()
#     # gr_test$param_set$values$filter_target_branch.selection = fb_
#     # gr_test$param_set$values$filter_target_id.q = 0.3
#     # gr_test$param_set$values$subsample.frac = 0.6
#     # gr_test$param_set$values$dropcorr.cutoff = 0.99
#     # gr_test$param_set$values$scale_branch.selection = sc_
#     return(gr_test$train(task_))
#   }
#
#   # test graph preprocesing
#   system.time({test_default = preprocess_test()})
#   # test_2 = preprocess_test(fb_ = "filter_target_select")
# }

# random forest graph
graph_rf = graph_template %>>%
  po("learner", learner = lrn("regr.ranger"))
graph_rf = as_learner(graph_rf)
as.data.table(graph_rf$param_set)[, .(id, class, lower, upper, levels)]
search_space_rf = search_space_template$clone()
search_space_rf$add(
  ps(regr.ranger.max.depth  = p_int(1, 15),
     regr.ranger.replace    = p_lgl(),
     regr.ranger.mtry.ratio = p_dbl(0.3, 1),
     regr.ranger.num.trees  = p_int(10, 2000),
     regr.ranger.splitrule  = p_fct(levels = c("variance", "extratrees")))
)

# xgboost graph
graph_xgboost = graph_template %>>%
  po("learner", learner = lrn("regr.xgboost"))
plot(graph_xgboost)
graph_xgboost = as_learner(graph_xgboost)
as.data.table(graph_xgboost$param_set)[grep("depth", id), .(id, class, lower, upper, levels)]
search_space_xgboost = ps(
  # filter target
  filter_target_branch.selection = p_fct(levels = c("nop_filter_target", "filter_target_select")),
  filter_target_id.q = p_fct(levels = c("0.4", "0.3", "0.2"),
                             trafo = function(x, param_set) return(as.double(x)),
                             depends = filter_target_branch.selection == "filter_target_select"),
  # subsample for hyperband
  subsample.frac = p_dbl(0.7, 1, tags = "budget"), # commencement this if we want to use hyperband optimization
  # preprocessing
  dropcorr.cutoff = p_fct(c("0.80", "0.90", "0.95", "0.99"),
                          trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_high = p_fct(as.character(winsorize_sp),
                                     trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_low = p_fct(as.character(1-winsorize_sp),
                                    trafo = function(x, param_set) return(as.double(x))),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # filters
  # learner
  regr.xgboost.alpha     = p_dbl(0.001, 100, logscale = TRUE),
  regr.xgboost.max_depth = p_int(1, 20),
  regr.xgboost.eta       = p_dbl(0.0001, 1, logscale = TRUE),
  regr.xgboost.nrounds   = p_int(1, 5000),
  regr.xgboost.subsample = p_dbl(0.1, 1)
)

# gbm graph
graph_gbm = graph_template %>>%
  po("learner", learner = lrn("regr.gbm"))
plot(graph_gbm)
graph_gbm = as_learner(graph_gbm)
as.data.table(graph_gbm$param_set)[, .(id, class, lower, upper, levels)]
search_space_gbm = search_space_template$clone()
search_space_gbm$add(
  ps(regr.gbm.distribution      = p_fct(levels = c("gaussian", "tdist")),
     regr.gbm.shrinkage         = p_dbl(lower = 0.001, upper = 0.1),
     regr.gbm.n.trees           = p_int(lower = 50, upper = 150),
     regr.gbm.interaction.depth = p_int(lower = 1, upper = 3))
  # ....
)

# catboost graph
graph_catboost = graph_template %>>%
  po("learner", learner = lrn("regr.catboost"))
graph_catboost = as_learner(graph_catboost)
as.data.table(graph_catboost$param_set)[, .(id, class, lower, upper, levels)]
search_space_catboost = search_space_template$clone()
# https://catboost.ai/en/docs/concepts/parameter-tuning#description10
search_space_catboost$add(
  ps(regr.catboost.learning_rate   = p_dbl(lower = 0.01, upper = 0.3),
     regr.catboost.depth           = p_int(lower = 4, upper = 10),
     regr.catboost.l2_leaf_reg     = p_int(lower = 1, upper = 5),
     regr.catboost.random_strength = p_int(lower = 0, upper = 3))
)

# cforest graph
graph_cforest = graph_template %>>%
  po("learner", learner = lrn("regr.cforest"))
graph_cforest = as_learner(graph_cforest)
as.data.table(graph_cforest$param_set)[, .(id, class, lower, upper, levels)]
search_space_cforest = search_space_template$clone()
# https://cran.r-project.org/web/packages/partykit/partykit.pdf
search_space_cforest$add(
  ps(regr.cforest.mtryratio    = p_dbl(0.05, 1),
     regr.cforest.ntree        = p_int(lower = 10, upper = 2000),
     regr.cforest.mincriterion = p_dbl(0.1, 1),
     regr.cforest.replace      = p_lgl())
)

# # gamboost graph
# # Error in eval(predvars, data, env) : object 'adxDx14' not found
# # This happened PipeOp regr.gamboost's $train()
# # In addition: There were 50 or more warnings (use warnings() to see the first 50)
# graph_gamboost = graph_template %>>%
#   po("learner", learner = lrn("regr.gamboost"))
# graph_gamboost = as_learner(graph_gamboost)
# as.data.table(graph_gamboost$param_set)[, .(id, class, lower, upper, levels)]
# search_space_gamboost = search_space_template$clone()
# search_space_gamboost$add(
#   ps(regr.gamboost.mstop       = p_int(lower = 10, upper = 100),
#      regr.gamboost.nu          = p_dbl(lower = 0.01, upper = 0.5),
#      regr.gamboost.baselearner = p_fct(levels = c("bbs", "bols", "btree")))
# )

# kknn graph
graph_kknn = graph_template %>>%
  po("learner", learner = lrn("regr.kknn"))
graph_kknn = as_learner(graph_kknn)
as.data.table(graph_kknn$param_set)[, .(id, class, lower, upper, levels)]
search_space_kknn = ps(
  # filter target
  filter_target_branch.selection = p_fct(levels = c("nop_filter_target", "filter_target_select")),
  filter_target_id.q = p_fct(levels = c("0.4", "0.3", "0.2"),
                             trafo = function(x, param_set) return(as.double(x)),
                             depends = filter_target_branch.selection == "filter_target_select"),
  # subsample for hyperband
  subsample.frac = p_dbl(0.7, 1, tags = "budget"), # commencement this if we want to use hyperband optimization
  # preprocessing
  dropcorr.cutoff = p_fct(c("0.80", "0.90", "0.95", "0.99"),
                          trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_high = p_fct(as.character(winsorize_sp),
                                     trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_low = p_fct(as.character(1-winsorize_sp),
                                    trafo = function(x, param_set) return(as.double(x))),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # filters,
  # learner
  regr.kknn.k        = p_int(lower = 1, upper = 50, logscale = TRUE),
  regr.kknn.distance = p_dbl(lower = 1, upper = 5),
  regr.kknn.kernel   = p_fct(levels = c("rectangular","optimal", "epanechnikov",
                                        "biweight", "triweight", "cos", "inv",
                                        "gaussian","rank"))
)

# nnet graph
graph_nnet = graph_template %>>%
  po("learner", learner = lrn("regr.nnet", MaxNWts = 50000))
graph_nnet = as_learner(graph_nnet)
as.data.table(graph_nnet$param_set)[, .(id, class, lower, upper, levels)]
search_space_nnet = search_space_template$clone()
search_space_nnet$add(
  ps(regr.nnet.size  = p_int(lower = 2, upper = 15),
     regr.nnet.decay = p_dbl(lower = 0.0001, upper = 0.1),
     regr.nnet.maxit = p_int(lower = 50, upper = 500))
)

# glmnet graph
graph_glmnet = graph_template %>>%
  po("learner", learner = lrn("regr.glmnet"))
graph_glmnet = as_learner(graph_glmnet)
as.data.table(graph_glmnet$param_set)[, .(id, class, lower, upper, levels)]
search_space_glmnet = ps(
  # filter target
  filter_target_branch.selection = p_fct(levels = c("nop_filter_target", "filter_target_select")),
  filter_target_id.q = p_fct(levels = c("0.4", "0.3", "0.2"),
                             trafo = function(x, param_set) return(as.double(x)),
                             depends = filter_target_branch.selection == "filter_target_select"),
  # subsample for hyperband
  subsample.frac = p_dbl(0.7, 1, tags = "budget"), # commencement this if we want to use hyperband optimization
  # preprocessing
  dropcorr.cutoff = p_fct(c("0.80", "0.90", "0.95", "0.99"),
                          trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_high = p_fct(as.character(winsorize_sp),
                                     trafo = function(x, param_set) return(as.double(x))),
  winsorizesimple.probs_low = p_fct(as.character(1-winsorize_sp),
                                    trafo = function(x, param_set) return(as.double(x))),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # filters
  # learner
  regr.glmnet.s     = p_int(lower = 5, upper = 30),
  regr.glmnet.alpha = p_dbl(lower = 1e-4, upper = 1, logscale = TRUE)
)


### THIS LEARNER UNSTABLE ####
# ksvm graph
# graph_ksvm = graph_template %>>%
#   po("learner", learner = lrn("regr.ksvm"), scaled = FALSE)
# graph_ksvm = as_learner(graph_ksvm)
# as.data.table(graph_ksvm$param_set)[, .(id, class, lower, upper, levels)]
# search_space_ksvm = ps(
#   # subsample for hyperband
#   subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
#   # preprocessing
#   dropcorr.cutoff = p_fct(
#     levels = c("0.80", "0.90", "0.95", "0.99"),
#     trafo = function(x, param_set) {
#       switch(x,
#              "0.80" = 0.80,
#              "0.90" = 0.90,
#              "0.95" = 0.95,
#              "0.99" = 0.99)
#     }
#   ),
#   # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
#   winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
#   winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
#   # filters
#   filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscov")),
#   # interaction
#   interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
#   # learner
#   regr.ksvm.kernel  = p_fct(levels = c("rbfdot", "polydot", "vanilladot",
#                                        "laplacedot", "besseldot", "anovadot")),
#   regr.ksvm.C       = p_dbl(lower = 0.0001, upper = 1000, logscale = TRUE),
#   regr.ksvm.degree  = p_int(lower = 1, upper = 5,
#                             depends = regr.ksvm.kernel %in% c("polydot", "besseldot", "anovadot")),
#   regr.ksvm.epsilon = p_dbl(lower = 0.01, upper = 1)
# )
### THIS LEARNER UNSTABLE ####

# LAST
# lightgbm graph
# [LightGBM] [Fatal] Do not support special JSON characters in feature name.
graph_lightgbm = graph_template %>>%
  po("learner", learner = lrn("regr.lightgbm"))
graph_lightgbm = as_learner(graph_lightgbm)
as.data.table(graph_lightgbm$param_set)[grep("sample", id), .(id, class, lower, upper, levels)]
search_space_lightgbm = search_space_template$clone()
search_space_lightgbm$add(
  ps(regr.lightgbm.max_depth     = p_int(lower = 2, upper = 10),
     regr.lightgbm.learning_rate = p_dbl(lower = 0.001, upper = 0.3),
     regr.lightgbm.num_leaves    = p_int(lower = 10, upper = 100))
)

# earth graph
graph_earth = graph_template %>>%
  po("learner", learner = lrn("regr.earth"))
graph_earth = as_learner(graph_earth)
as.data.table(graph_earth$param_set)[grep("sample", id), .(id, class, lower, upper, levels)]
search_space_earth = search_space_template$clone()
search_space_earth$add(
  ps(regr.earth.degree  = p_int(lower = 1, upper = 4),
     # regr.earth.penalty = p_int(lower = 1, upper = 5),
     regr.earth.nk      = p_int(lower = 50, upper = 250))
)

# rsm graph
# TODO It need lots of memory ?
graph_rsm = graph_template %>>%
  po("learner", learner = lrn("regr.rsm"))
plot(graph_rsm)
graph_rsm = as_learner(graph_rsm)
as.data.table(graph_rsm$param_set)[, .(id, class, lower, upper, levels)]
search_space_rsm = search_space_template$clone()
search_space_rsm$add(
  ps(regr.rsm.modelfun = p_fct(levels = c("FO", "TWI", "SO")))
)
# Error in fo[, i] * fo[, j] : non-numeric argument to binary operator
# This happened PipeOp regr.rsm's $train()
# Calls: lapply ... resolve.list -> signalConditionsASAP -> signalConditions
# In addition: Warning message:
# In bbandsDn5:volumeDownUpRatio :
#   numerical expression has 4076 elements: only the first used
# This happened PipeOp regr.rsm's $train()

# BART graph
graph_bart = graph_template %>>%
  po("learner", learner = lrn("regr.bart"))
graph_bart = as_learner(graph_bart)
as.data.table(graph_bart$param_set)[, .(id, class, lower, upper, levels)]
search_space_bart = search_space_template$clone()
search_space_bart$add(
  ps(regr.bart.k      = p_int(lower = 1, upper = 10),
     regr.bart.numcut = p_int(lower = 10, upper = 200),
     regr.bart.ntree  = p_int(lower = 50, upper = 500))
)
# chatgpt returns this
# n_chains = p_int(lower = 1, upper = 5),
# m_try = p_int(lower = 1, upper = 13),
# nu = p_dbl(lower = 0.1, upper = 10),
# alpha = p_dbl(lower = 0.01, upper = 1),
# beta = p_dbl(lower = 0.01, upper = 1),
# burn = p_int(lower = 10, upper = 100),
# iter = p_int(lower = 100, upper = 1000)

# Threads
# if (interactive()) {
#   threads = 4
# } else {
#   threads = 4
# }
threads = 4
set_threads(graph_rf, n = threads)
set_threads(graph_xgboost, n = threads)
set_threads(graph_bart, n = threads)
# set_threads(graph_ksvm, n = threads) # unstable
set_threads(graph_nnet, n = threads)
set_threads(graph_kknn, n = threads)
set_threads(graph_lightgbm, n = threads)
set_threads(graph_earth, n = threads)
set_threads(graph_gbm, n = threads)
set_threads(graph_rsm, n = threads)
set_threads(graph_catboost, n = threads)
set_threads(graph_glmnet, n = threads)
# set_threads(graph_cforest, n = threads)


# DESIGNS -----------------------------------------------------------------
designs_l = lapply(custom_cvs, function(cv_) {
  # debug
  # cv_ = custom_cvs[[1]]
  
  # get cv inner object
  cv_inner = cv_$inner
  cv_outer = cv_$outer
  cat("Number of iterations fo cv inner is ", cv_inner$iters, "\n")
  
  to_ = cv_inner$iters
  designs_cv_l = lapply(to_, function(i) { # 1:cv_inner$iters
    # debug
    # i = 1
    
    # with new mlr3 version I have to clone
    task_inner = task$clone()
    task_inner$filter(c(cv_inner$train_set(i), cv_inner$test_set(i)))
    
    # inner resampling
    custom_ = rsmp("custom")
    custom_$id = paste0("custom_", cv_inner$iters, "_", i)
    custom_$instantiate(task_inner,
                        list(cv_inner$train_set(i)),
                        list(cv_inner$test_set(i)))
    
    # objects for all autotuners
    measure_ = msr("adjloss2")
    tuner_   = tnr("hyperband", eta = 6)
    # tuner_   = tnr("mbo")
    # term_evals = 20
    
    # auto tuner rf
    at_rf = auto_tuner(
      tuner = tuner_,
      learner = graph_rf,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_rf,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner xgboost
    at_xgboost = auto_tuner(
      tuner = tuner_,
      learner = graph_xgboost,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_xgboost,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner BART
    at_bart = auto_tuner(
      tuner = tuner_,
      learner = graph_bart,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_bart,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner nnet
    at_nnet = auto_tuner(
      tuner = tuner_,
      learner = graph_nnet,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_nnet,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner lightgbm
    at_lightgbm = auto_tuner(
      tuner = tuner_,
      learner = graph_lightgbm,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_lightgbm,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner earth
    at_earth = auto_tuner(
      tuner = tuner_,
      learner = graph_earth,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_earth,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner kknn
    at_kknn = auto_tuner(
      tuner = tuner_,
      learner = graph_kknn,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_kknn,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner gbm
    at_gbm = auto_tuner(
      tuner = tuner_,
      learner = graph_gbm,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_gbm,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner rsm
    at_rsm = auto_tuner(
      tuner = tuner_,
      learner = graph_rsm,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_rsm,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner rsm
    at_bart = auto_tuner(
      tuner = tuner_,
      learner = graph_bart,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_bart,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner catboost
    at_catboost = auto_tuner(
      tuner = tuner_,
      learner = graph_catboost,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_catboost,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner glmnet
    at_glmnet = auto_tuner(
      tuner = tuner_,
      learner = graph_glmnet,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_glmnet,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # auto tuner glmnet
    at_cforest = auto_tuner(
      tuner = tuner_,
      learner = graph_cforest,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_cforest,
      terminator = trm("none")
      # term_evals = term_evals
    )
    
    # outer resampling
    customo_ = rsmp("custom")
    customo_$id = paste0("custom_", cv_inner$iters, "_", i)
    customo_$instantiate(task, list(cv_outer$train_set(i)), list(cv_outer$test_set(i)))
    
    # nested CV for one round
    design = benchmark_grid(
      tasks = task,
      learners = list(at_rf, at_xgboost, at_lightgbm, at_nnet, at_earth,
                      at_kknn, at_gbm, at_rsm, at_bart, at_catboost, at_glmnet), # , at_cforest
      resamplings = customo_
    )
  })
  designs_cv = do.call(rbind, designs_cv_l)
})
designs = do.call(rbind, designs_l)

# If MODEL is TRUE, train the model
if (MODEL) {
  # Exp dir
  date_ = strftime(Sys.time(), format = "%Y%m%d")
  dirname_ = glue("experiments_pread_live_{date_}")
  if (dir.exists(dirname_)) system(paste0("rm -r ", dirname_))
  
  # Create registry
  packages = c("data.table", "gausscov", "paradox", "mlr3", "mlr3pipelines",
               "mlr3tuning", "mlr3misc", "future", "future.apply",
               "mlr3extralearners", "stats")
  reg = makeExperimentRegistry(file.dir = dirname_, seed = 1, packages = packages)
  
  # Populate registry with problems and algorithms to form the jobs
  batchmark(designs, store_models = TRUE, reg = reg)
  
  # Save registry
  saveRegistry(reg = reg)
  
  # get nondone jobs
  ids = findNotDone(reg = reg)
  
  # set up cluster (for local it is parallel)
  cf = makeClusterFunctionsSocket(ncpus = 4L)
  reg$cluster.functions = cf
  saveRegistry(reg = reg)
  
  # define resources and submit jobs
  resources = list(ncpus = 2, memory = 8000)
  submitJobs(ids = ids$job.id, resources = resources, reg = reg)
}


# PREDICTIONS -------------------------------------------------------------
# get data for last date
data_ = task$backend$data(rows = task$row_ids, cols = c("..row_id", id_cols))
data_[, max(date)]
data_ = data_[date == max(date)]
setnames(data_, "..row_id", "row_ids")

# get last registry folder
live_dirs = dir_ls(regexp = "experiments_pread_live")
dates_ = as.Date(gsub(".*?(\\d+)", "\\1", live_dirs), format = "%Y%m%d")
last_file = names(dates_[which.max(dates_)])

# read registriy
reg = loadRegistry(last_file)

# If MODEL just extract the results, otherwise predict on new data
if (MODEL) {
  # extract results
  results = reduceResultsBatchmark(reg = reg, store_backends = TRUE)
  task_ = results$tasks$task[[1]]
  backend_ = task_$backend$data(rows = task_$row_ids, cols = c("..row_id", id_cols))
  backend_last = backend_[date == max(date)]
  setnames(backend_last, "..row_id", "row_ids")
  
  # extract predictions
  predictions = lapply(1:results$n_resample_results, function(i) {
    # i = 1
    lid = gsub(".*\\.regr.|\\.tuned", "", results$learners$learner_id)[i]
    predictions_ = results$resample_results$resample_result[[i]]$predictions()[[1]]
    predictions_ = as.data.table(predictions_)
    predictions_ = predictions_[backend_last, on = "row_ids"]
    cbind(learner = lid, predictions_)
  })
  predictions = rbindlist(predictions, fill = TRUE)
  predictions[!is.na(response.V1), response := response.V1]
  predictions[, response.V1 := NULL]
} else {
  # task for new data
  task_new = task$clone()
  task_new$filter(rows = data_[, row_ids])
  backend_last = task_new$backend$data(rows = task_new$row_ids, cols = c("..row_id", id_cols))
  setnames(backend_last, "..row_id", "row_ids_backend")
  
  # import model
  models = dir_ls(path(last_file, "results"))
  predictions_l = lapply(1:length(models), function(i) {
    # i = 4
    model_ = readRDS(models[i])
    
    ########### CHECK ############
    test = task_new$data()
    # list all NA columns
    na_cols = test[, lapply(.SD, function(x) sum(is.na(x)))]
    colnames(test)[na_cols == nrow(test)]
    
    # test = test[, lapply(.SD, function(x) ifelse(is.na(x), runif(1), x))]
    # cols_ = colnames(test)[grep("tsfreshValuesLinearTrendAttrP", colnames(test), ignore.case = TRUE)]
    # test = test[, (cols_) := lapply(.SD, function(x) ifelse(is.na(x), runif(1), x)), .SDcols = cols_]
    predictions_= model_$learner_state$model$learner$predict_newdata(newdata = test)
    test[, grepl("tsfreshValuesLinearTrendAttr", colnames(test))]
    ########### CHECK ############
    
    predictions_ = as.data.table(predictions_)
    learner_ = model_$learner_state$model$learner$base_learner()$base_learner()
    lid = learner_$id
    cbind(learner = lid, predictions_, backend_last)
  })
  predictions = rbindlist(predictions_l, fill = TRUE)
  predictions[, learner := gsub("regr\\.", "", learner)]
  if ("response.V1" %in% colnames(predictions)) {
    predictions[is.na(response) & !is.na(response.V1), response := response.V1]  
    predictions[, response.V1 := NULL]
  }
}

# Save predictions for potential future use
time_ = strftime(Sys.time(), format = "%Y%m%d")
file_name = glue("pread_predictions_{time_}.csv")
fwrite(predictions, path(SAVEPATH, file_name))

# Save the results to the Azure
qc_data = dcast(predictions, symbol ~ learner, value.var = "response")
qc_data = as.data.frame(qc_data)
rownames(qc_data) = NULL
storage_write_csv(qc_data, cont, "pread_live.csv" )
