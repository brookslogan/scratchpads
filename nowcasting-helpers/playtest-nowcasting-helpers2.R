
library(dplyr)
library(tidyr)
library(purrr)
library(epidatr)

cce <- covidcast_epidata()

beginning_of_time <- as.Date("1234-01-01")
analysis_date <- as.Date("2026-03-15")

admissions_issues <- cce$signals$"hhs:confirmed_admissions_covid_1d"$call("state", "*", "*", issues = epirange(beginning_of_time, analysis_date))

google_issues <- cce$signals$"google-symptoms:s02_smoothed_search"$call("state", "*", "*", issues = epirange(beginning_of_time, analysis_date))

full_archive <- epix_merge(
  admissions_issues %>%
    select(geo_value, time_value, version = issue, admissions = value) %>%
    as_epi_archive(),
  google_issues %>%
    select(geo_value, time_value, version = issue, google = value) %>%
    as_epi_archive(),
  sync = "locf"
)

nowcast_version <- as.Date("2024-02-02")
archive <- full_archive %>%
  epix_as_of(nowcast_version, all_versions = TRUE)


testing_reference_time <- version_get_containing_time_value(nowcast_version, archive)
latest <- archive %>% epix_as_of_latest()


target <- "admissions"
target_offset <- 7

predictors <- c("admissions", "google")




stopifnot(target %in% predictors)

predictors_diffkeys <- predictors %>%
  `names<-`({.}) %>%
  lapply(function(predictor) {
    epix_diffkeys(archive, predictor)
  })

predictors_vtol <- predictors_diffkeys %>%
  lapply(function(diffkeys) {
    vtol_preprocess(NULL, archive, diffkeys)
  })

sum_features_spec <- bind_rows(
  tibble(predictor = "admissions", base_offset = 0, add_offset = 7 * -3:3, before = 6, after = 0),
  tibble(predictor = "google", base_offset = -7*14, add_offset = 7 * -3:3, before = 0, after = 0),
  )

sum_features_respec <- sum_features_spec %>%
  arrange(abs(add_offset)) %>%
  mutate(ref_offset = base_offset + add_offset + target_offset,
         .keep = "unused") %>%
  mutate(feature = glue::glue_data(., "{predictor}_tlag{ref_offset}_sum{before}_{after}"))

training_tbl <- archive %>%
  epix_target_evaluation_data(target, -target_offset, anchor_versions = unique(.$DT$version)) %>%
  na.omit() %>%
  mutate(reference_time = anchor_version - target_offset)

# for(sum_feature_i in seq_len(nrow(sum_features_respec))) {
# }

lapply(seq_len(nrow(sum_features_respec)), function(sum_feature_i) {
  predictor <- sum_features_respec$predictor[[sum_feature_i]]
  ref_offset <- sum_features_respec$ref_offset[[sum_feature_i]]
  before <- sum_features_respec$before[[sum_feature_i]]
  after <- sum_features_respec$after[[sum_feature_i]]
  latest %>%
    select(all_of(c(key_colnames(.), predictor))) %>%
    filter(between(.data$time_value,
                   .env$testing_reference_time + .env$ref_offset - .env$before,
                   .env$testing_reference_time + .env$ref_offset + .env$after)) %>%
    filter(!is.na(.data[[predictor]])) %>%
    mutate(version = corresponding_diffkey_versions(., archive, predictor)) %>%
    select(-all_of(predictor)) %>%
    pack(ek = all_of(key_colnames(latest, exclude = "time_value")),
         tv = all_of(c("time_value", "version"))) %>%
    chop("tv") %>%
    mutate(predictor = .env$predictor) %>%
    {}
}) %>%
  bind_rows() %>%
  nest(agg_feature = c(predictor, tv)) %>%
  # TODO complete eks
  chop(ek) %>%
  # {
  #   lapply(seq_len(nrow(.)), function(i) {
  #     as.list(.[i,])
  #   })
  # } %>%
  mutate(
    agg_feature = map_chr(agg_feature, function(some_agg_feature) {
      tv_chr <- map_chr(some_agg_feature$tv, function(some_tv) {
        paste0("(", some_tv$time_value, ", ", some_tv$version, ")") %>%
          paste(collapse = ", ")
      })
      paste0(some_agg_feature$predictor, " @ ", tv_chr) %>%
        paste(collapse = ", ")
    })
    # TODO map ekset -> str
  ) %>%
  {}

sum_features_respec %>%
  filter(predictor == "admissions") %>%
  mutate(testing_start = testing_reference_time + ref_offset - before,
         testing_end = testing_reference_time + ref_offset + after) %>%
  nest_join(latest, join_by(between(y$time_value, x$testing_start, x$testing_end)))
