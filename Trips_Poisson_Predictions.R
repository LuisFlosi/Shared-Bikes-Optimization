pkgs <- c('doParallel', 'foreach')
lapply(pkgs, require, character.only = T)
#Find out how many cores are available (if you don't already know)
cores<-detectCores()
#Create cluster with desired number of cores, leave one open for the machine         
#core processes
cl <- makeCluster(cores[1]-1)
#Register cluster
registerDoParallel(cl)
set.seed(02252020)

arriving = read.csv("~/arriving_structuredaaa.csv",sep = "/")
arriving["station_id"] = lapply(arriving["station_id"], as.factor)
arriving["end_hour"] = lapply(arriving["end_hour"], as.factor)
arriving["end_month"] = lapply(arriving["end_month"], as.factor)
arr_lim_cols = arriving[c("station_id", "end_hour", "end_month", "arriving_trips", "tempC", "visibility", "day_of_week")]

leaving = read.csv(paste(dataPath,"leaving_structuredlll.csv",sep = "/"))

leaving["station_id"] = lapply(leaving["station_id"],as.factor)
leaving["start_hour"] = lapply(leaving["start_hour"],as.factor)
leaving["start_month"] = lapply(leaving["start_month"],as.factor)
leav_lim_cols = leaving[c("station_id", "start_hour", "start_month", "leaving_trips", "tempC", "visibility", "day_of_week")]

sz = 0.05*nrow(leav_lim_cols)

train_leav = leav_lim_cols[sample(nrow(leav_lim_cols), sz), ]
tab <- table(train_leav$station_id)
train_leav = train_leav[train_leav$station_id %in% names(tab)[tab>50],]

train_arr = arr_lim_cols[sample(nrow(arr_lim_cols), sz), ]
train_arr = train_arr[train_arr$station_id %in% names(tab)[tab>50],]

train_leav = leav_lim_cols

train_arr = arr_lim_cols

arr_pred = glm(arriving_trips ~ ., data = train_arr, family = "poisson")

leav_pred = glm(leaving_trips ~ ., data = train_leav, family = "poisson")


test = expand.grid(as.factor(unlist(unique(train_arr["station_id"]))),
                   as.factor(c(0:23)),
                   as.factor(c(7)),
                   c(25),
                   c(10),
                   c("Weekday"))

colnames(test) = c("station_id","end_hour","end_month","tempC","visibility","day_of_week")

arrive_preds = predict(arr_pred, newdata = test, type = "response")

test_arr = test

test_arr["predicted_arr"] = arrive_preds

test = expand.grid(as.factor(unlist(unique(train_leav["station_id"]))),
                   as.factor(c(0:23)),
                   as.factor(c(7)),
                   c(25),
                   c(10),
                   c("Weekday"))

colnames(test) = c("station_id","start_hour","start_month","tempC","visibility","day_of_week")

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
station_hourly_preds[7:9] = lapply(station_hourly_preds[7:9], round)

# Save results

res <- list(leave_preds=leave_preds,
            arrive_preds=arrive_preds,
            station_hourly_preds=station_hourly_preds,
            boot_pi = boot_pi)
saveRDS(res, file = paste(dataPath,'result.rds',sep = '/'))    