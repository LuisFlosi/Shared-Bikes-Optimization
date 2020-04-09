pkgs <- c('doParallel', 'foreach')
lapply(pkgs, require, character.only = T)
cores<-detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)

# Data prep
set.seed(04072020)

arriving = read.csv("~/MScA/3. Optimization and Simulation/Final Project/arriving_structured.csv")
arriving["station_id"] = lapply(arriving["station_id"], as.factor)
arriving["end_hour"] = lapply(arriving["end_hour"], as.factor)
arriving["end_month"] = lapply(arriving["end_month"], as.factor)
arr_lim_cols = arriving[c("station_id", "end_hour", "end_month", "arriving_trips", "tempC", "visibility", "day_of_week")]

leaving = read.csv("~/MScA/3. Optimization and Simulation/Final Project/leaving_structured.csv")

leaving["station_id"] = lapply(leaving["station_id"],as.factor)
leaving["start_hour"] = lapply(leaving["start_hour"],as.factor)
leaving["start_month"] = lapply(leaving["start_month"],as.factor)
leav_lim_cols = leaving[c("station_id", "start_hour", "start_month", "leaving_trips", "tempC", "visibility", "day_of_week")]

sz = round(0.005*nrow(leav_lim_cols))

train_leav = leav_lim_cols[sample(nrow(leav_lim_cols), sz), ]
tab <- table(train_leav$station_id)
train_leav = train_leav[train_leav$station_id %in% names(tab)[tab>10],]

train_arr = arr_lim_cols[sample(nrow(arr_lim_cols), sz), ]
train_arr = train_arr[train_arr$station_id %in% names(tab)[tab>10],]

set.seed(07042020)
test_leav = leav_lim_cols[sample(nrow(leav_lim_cols), sz), ]
test_leav = test_leav[test_leav$station_id %in% names(tab)[tab>10],]
test_arr = arr_lim_cols[sample(nrow(arr_lim_cols), sz), ]
test_arr = test_arr[test_arr$station_id %in% names(tab)[tab>10],]

weekdays = function(dta){
  for (i in dta) {  
    if (i %in% c("Monay","Tuesday","Wednesday","Thursday","Friday")){
      return("Weekday")
    } else {
      return("Weekend")
    }
  }
}

hour_group = function(i){
    if (i %in% c(0,1,2,3,4,5,6,7)){
      return(as.factor("0"))
    } else if (i %in% c(8,9,10)) {
      return(as.factor("8"))
    } else if (i %in% c(11,12,13,14,15,16)) {
      return(as.factor("11"))
    } else if (i %in% c(17,18,19)) {
      return(as.factor("17"))
    } else {
      return(as.factor("20"))
    }
}


train_arr["days_of_week"] = sapply(train_arr$day_of_week, weekdays)
train_arr["days_of_week"] = sapply(train_arr$days_of_week, as.factor)
train_arr["day_of_week"] = NULL
train_arr["hour_group"] = sapply(as.numeric(train_arr$end_hour),hour_group)
train_leav["days_of_week"] = sapply(train_leav$day_of_week, weekdays)
train_leav["days_of_week"] = sapply(train_leav$days_of_week, as.factor)
train_leav["day_of_week"] = NULL
train_leav["hour_group"] = sapply(as.numeric(train_leav$start_hour),hour_group)

test_arr["days_of_week"] = sapply(test_arr$day_of_week, weekdays)
test_arr["days_of_week"] = sapply(test_arr$days_of_week, as.factor)
test_arr["day_of_week"] = NULL
test_arr["hour_group"] = sapply(as.numeric(test_arr$end_hour),hour_group)

test_leav["days_of_week"] = sapply(test_leav$day_of_week, weekdays)
test_leav["days_of_week"] = sapply(test_leav$days_of_week, as.factor)
test_leav["day_of_week"] = NULL
test_leav["hour_group"] = sapply(as.numeric(test_leav$start_hour),hour_group)



# Arrivals model
arr_pred = glm(arriving_trips ~  station_id*hour_group + hour_group + tempC + station_id*days_of_week, data = train_arr, family = "poisson")
summary(arr_pred)
arr_pred$deviance / arr_pred$df.residual # no over dispersion
pchisq(arr_pred$deviance, arr_pred$df.residual, lower.tail=F) # shows bad fit

test_arr["predicted_arr"] = ceiling(predict(arr_pred, newdata = test_arr))
test_arr["arr_error"] = test_arr$arriving_trips - test_arr$predicted_arr
arr_errs = test_arr$arr_error
hist(arr_errs[arr_errs < 20], main ="distribution of arrival errors", xlab = "error") # right-tailed errors, with -1 to 5 as the range


# Departures (Leaving) predictions
leav_pred = glm(leaving_trips ~ station_id*hour_group + hour_group + tempC + station_id*days_of_week, data = train_leav, family = "poisson")
summary(leav_pred)
leav_pred$deviance / leav_pred$df.residual # no over dispersion
pchisq(leav_pred$deviance, leav_pred$df.residual, lower.tail=F) # shows bad fit

test_leav["predicted_leav"] = ceiling(predict(leav_pred, newdata = test_leav))
test_leav["leav_error"] = test_leav$leaving_trips - test_leav$predicted_leav
leav_errs = test_leav$leav_error
hist(leav_errs[leav_errs < 20], main ="distribution of departure errors", xlab = "error") # right-tailed errors, with -1 to 5 as the range

par(mfrow=c(1,2))
hist(arr_errs[arr_errs < 20], main ="distribution of arrival errors", xlab = "error") # right-tailed errors, with -1 to 5 as the range
hist(leav_errs[leav_errs < 20], main ="distribution of departure errors", xlab = "error") # right-tailed errors, with -1 to 5 as the range

# Predict "standard case"
test = expand.grid(as.factor(unlist(unique(train_arr["station_id"]))),
                   as.factor(c(0:23)),
                   as.factor(c(7)),
                   c(25),
                   c(10),
                   c("Weekday"))

colnames(test) = c("station_id","end_hour","end_month","tempC","visibility","days_of_week")

arrive_preds = predict(arr_pred, newdata = test, type = "response")

colnames(test) = c("station_id","start_hour","start_month","tempC","visibility","days_of_week")

test_arr = test

test_arr["predicted_arr"] = arrive_preds

# Bootstraping function from https://www.r-bloggers.com/prediction-intervals-for-poisson-regression/
boot_pi <- function(model, pdata, n, p) {
  odata <- model$data
  lp <- (1 - p) / 2
  up <- 1 - lp
  set.seed(2016)
  seeds <- round(runif(n, 1, 1000), 0)
  boot_y <- foreach(i = 1:n, .combine = rbind) %dopar% {
    set.seed(seeds[i])
    bdata <- odata[sample(seq(nrow(odata)), size = nrow(odata), replace = TRUE), ]
    bpred <- predict(update(model, data = bdata), type = "response", newdata = pdata)
    rpois(length(bpred), lambda = bpred)
  }
  boot_ci <- t(apply(boot_y, 2, quantile, c(lp, up)))
  return(data.frame(pred = predict(model, newdata = pdata, type = "response"), lower = boot_ci[, 1], upper = boot_ci[, 2]))
}

leave_preds = boot_pi(leav_pred, test, 10, 0.95)

test_leav = test

test_leav["predicted_leav"] = leave_preds[1]
test_leav["predicted_leav_upper"] = leave_preds[3]

colnames(test_arr) = c("station_id","start_hour","start_month","tempC","visibility","day_of_week", "predicted_arr")

station_hourly_preds = merge(test_arr, test_leav, on = c("station_id", "start_hour", "start_month", "tempC", "visibility", "day_of_week"))
station_hourly_preds[7:9] = lapply(station_hourly_preds[7,9,10], round)

# Save results

res <- list(leave_preds=leave_preds,
            arrive_preds=arrive_preds,
            station_hourly_preds=station_hourly_preds)
saveRDS(res, file = '~/MScA/3. Optimization and Simulation/Final Project/test_preds.rds')    