
library(dplyr)
library(tidyr)
library(purrr)
library(epidatr)
library(vctrs)
library(magrittr)
source("playtest-utils.R")
source("vars.R")

# archives_data <- list(
#   epidata_archive("nhsn", "confirmed_admissions_covid_ew", "state"),
#   epidata_archive("nssp", "pct_ed_visits_covid", "state")
# )

# full_archive <- bind_rows(archives_data) %>%
#   as_epi_archive()

# need changes from dev to make above work

full_archive <-
  archives_data %>%
  lapply(function(x) {
    x %>%
      mutate(report_time = {
        # fix up fake midnight UTC
        report_date <- as.Date(as.character(report_time))
        # check that was fake midnight UTC
        stopifnot(identical(as.POSIXct(report_date), report_time))
        report_date
      })
  }) %>%
  lapply(function(x) pivot_wider(x, id_cols = c("geo_value", "reference_time", "report_time"), names_from = "signal", values_from = "value")) %>%
  lapply(function(x) as_epi_archive(x, time_value = reference_time, version = report_time)) %>%
  Reduce(f = function(x, y) epix_merge(x, y, sync = "locf")) %>%
  set_time_week_end("Sat") %>%
  {}

###########

nowcast_version <- as.Date("2025-02-05")
archive <- full_archive %>%
  epix_as_of(nowcast_version, all_versions = TRUE) %>%
  filter(geo_value == unique(geo_value)[[2L]])

testing_reference_time <- version_get_containing_time_value(nowcast_version, archive)
latest <- archive %>% epix_as_of_latest()
ek_names <- key_colnames(archive, exclude = c("time_value", "version"))
ekt_names <- c(ek_names, "time_value")
time_type <- archive$time_type

target <- "confirmed_admissions_covid_ew"
target_offset <- 7
predictors <- c("confirmed_admissions_covid_ew", "pct_ed_visits_covid")
testing_target_time <- testing_reference_time + target_offset

features_spec <- bind_rows(
  tibble(predictor = "confirmed_admissions_covid_ew", base_offset = 0),
  tibble(predictor = "pct_ed_visits_covid", base_offset = 0),
  ) %>%
  mutate(max_abs_additional_offset = as.difftime(60, units = "days"))

for (features_spec_row_i in seq_len(nrow(features_spec))) {
  features_spec_row <- extract_row_as_list(features_spec, features_spec_row_i)
  print(features_spec_row)
  predictor <- features_spec_row$predictor
  base_offset <- features_spec_row$base_offset
  max_abs_additional_offset <- features_spec_row$max_abs_additional_offset %>%
    as_inclusive_if_not_bound() %>%
    # ^ except never are bound, as bounds not vectors right now, but max abs add offset stored as vector
    {
      if (inherits(.[["threshold"]], "difftime")) {
        .[["threshold"]] <- epiprocess:::difftime_approx_ceiling_time_delta(.[["threshold"]], time_type)
        .
      } else {
        .
      }
    }
  latest %>%
    extract(in_bound(abs(.$time_value - (testing_target_time + base_offset)),
                     max_abs_additional_offset),
            c(ekt_names, features_spec_row$predictor)
            ) %>%
    print()
}
