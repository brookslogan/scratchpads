
library(dplyr)
library(epidatr)
library(epiprocess)

epidata_meta("nhsn")$signals

analysis_as_of <- as.Date("2026-09-01")

# snapshot <- epidata("nhsn", "confirmed_admissions_covid_ew", "state", snapshot_date = analysis_as_of)
snapshot <- epidata("nhsn", "confirmed_admissions_flu_ew", "state", snapshot_date = analysis_as_of)

snapshot %>%
  as_epi_df() %>%
  # summary() %>%
  {}

edf <- snapshot %>%
  as_epi_df() %>%
  # filter(geo_value == "ga") %>%
  filter(geo_value == "ca") %>%
  # filter(time_value >= as.Date("2022-08-01"))
  filter(time_value >= as.Date("2023-05-01"))

# edf %>%
#   autoplot()

dat <-
  edf %>%
  epi_slide(function(x, gk, rtv) {
    list(tibble(
      var = paste0("Y", seq_along(x$value)),
      value = x$value
    ))
  }, .window_size = as.difftime(8, units = "weeks")) %>%
  select(time_value, slide_value) %>%
  unnest(slide_value) %>%
  pivot_wider(id_cols = "time_value", names_from = "var", values_from = "value") %>%
  na.omit() %>%
  mutate(across(starts_with("Y"), list(log1p = log1p))) %>%
  {}

fit <- dat %>%
  # glm(formula = Y8 ~ Y1 + Y2 + Y3 + Y4 + Y1_log1p + Y2_log1p + Y3_log1p + Y4_log1p, family = poisson("log")) %>%
  # glm(formula = Y8 ~ Y1_log1p + Y2_log1p + Y3_log1p + Y4_log1p, family = poisson("log")) %>%
  # glm(formula = Y8 ~ Y1 + Y2 + Y3 + Y4, family = poisson("identity"), start = c(1, rep(1,4)/4)) %>%
  glm(formula = Y8 ~ Y7/Y6*(Y7-Y6) + Y7 + Y7^2 + Y7*Y6 + 0, family = poisson("identity")) %>%
  # FIXME ^ zeros probably make ^ not work, for ga; but does for ca, good
  # glm(formula = Y8 ~ Y7/Y6*(Y7-Y6) + Y7 + Y7^2 + Y7*Y6, family = poisson("identity")) %>%
  # ^ this still doesn't work though, for ga
  # glm(formula = Y8 ~ Y7/Y6*(Y7-Y6) + Y7 + Y7^2 + Y7*Y6 + 0, family = poisson("identity")) %>%
  {}

fit

# plot(fit)

state <- dat %>% tail(1L)
preds <- numeric(100)
for (i in 1:(52*4)) {
  state[, paste0("Y", 1:7)] <- state[, paste0("Y", 2:8)]
  # state[, "Y8"] <- NA
  # state[, paste0("Y", 1:7)] <- state[, paste0("Y", 2:8, "_log1p")]
  # state[, "Y8_log1p"] <- NA
  # pred <- rpois(1L, exp(predict(fit, newdata = state)))
  pred <- rpois(1L, predict(fit, newdata = state))
  state[, "Y8"] <- pred
  # state[, "Y8_log1p"] <- log1p(pred)
  preds[[i]] <- pred
}
plot(preds)
lines(preds)
# ^ fit fails to reproduce "real" waves

gamma <- 1/(6/7) # https://www.webmd.com/cold-and-flu/how-long-flu-contagious ignoring latent period
beta <- 1.3*gamma # https://en.wikipedia.org/wiki/Basic_reproduction_number
mu <- 1/26 # randomly pulling a hypothetical referenced in https://www.sciencedirect.com/science/article/pii/S0264410X23007132
N <- 1e6 # whatever
rho <- 1 # whatever

preds <- numeric(100)
state <- c(Y1 = 100, Y2 = 100)
# for (i in 1:2000) {
for (i in 1:(4*52)) {
  Y1 <- state[["Y1"]]
  Y2 <- state[["Y2"]]
  state[["Y1"]] <- Y2
  state[["Y2"]] <- rpois(1L, (1-mu)*Y2/Y1*(Y2-Y1) + (beta*mu - gamma*mu + 1)*Y2 - beta/(N*rho)*Y2^2 - beta/(N*rho)*(gamma  + mu - 1)*Y2*Y1)
  preds[[i]] <- state[["Y2"]]
}
plot(preds)
# ^ does consistently have waves etc.

# TODO smoothed versions of some features?

# also missing seasonal forcing...
