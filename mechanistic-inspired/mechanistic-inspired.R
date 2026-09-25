
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
  filter(geo_value == "ga") %>%
  # filter(time_value >= as.Date("2022-08-01"))
  filter(time_value >= as.Date("2023-05-01"))

edf %>%
  autoplot()

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
  glm(formula = Y8 ~ Y1 + Y2 + Y3 + Y4 + Y1_log1p + Y2_log1p + Y3_log1p + Y4_log1p, family = poisson("log")) %>%
  # glm(formula = Y8 ~ Y1_log1p + Y2_log1p + Y3_log1p + Y4_log1p, family = poisson("log")) %>%
  # glm(formula = Y8 ~ Y1 + Y2 + Y3 + Y4, family = poisson("identity"), start = c(1, rep(1,4)/4)) %>%
  {}

fit

# plot(fit)

state <- dat %>% tail(1L)
preds <- numeric(100)
for (i in 1:100) {
  state[, paste0("Y", 1:7)] <- state[, paste0("Y", 2:8)]
  state[, "Y8"] <- NA
  state[, paste0("Y", 1:7)] <- state[, paste0("Y", 2:8, "_log1p")]
  state[, "Y8_log1p"] <- NA
  pred <- rpois(1L, exp(predict(fit, newdata = state)))
  state[, "Y8"] <- pred
  state[, "Y8_log1p"] <- log1p(pred)
  preds[[i]] <- pred
}

plot(preds)

# this was attempt from noiseless diffeq stuff; might have done wrong,
# or maybe from noiseless and constant-param assumptions...
