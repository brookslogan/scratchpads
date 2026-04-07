
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

predictors_confkeys <- predictors %>%
  `names<-`({.}) %>%
  lapply(function(predictor) {
    epix_confkeys(archive, predictor)
  })

predictors_vtol <- predictors_confkeys %>%
  lapply(function(confkeys) {
    vtol_preprocess(NULL, archive, confkeys)
  })

sum_features_spec <- bind_rows(
  tibble(predictor = "admissions", base_offset = 0, add_offset = 7 * -3:3, before = 6, after = 0),
  tibble(predictor = "google", base_offset = -7*2, add_offset = 7 * -3:3, before = 0, after = 0),
  )

sum_features_respec <- sum_features_spec %>%
  arrange(abs(add_offset)) %>%
  mutate(feature_ref_offset = base_offset + add_offset + target_offset,
         .keep = "unused") %>%
  # TODO get slide naming convention, indicate partial
  mutate(feature = glue::glue_data(., "{predictor}_relt_{feature_ref_offset}_sum_{before}_{after}"))

# for(sum_feature_i in seq_len(nrow(sum_features_respec))) {
# }

lapply(seq_len(nrow(sum_features_respec)), function(sum_feature_i) {
  feature <- sum_features_respec$feature[[sum_feature_i]]
  predictor <- sum_features_respec$predictor[[sum_feature_i]]
  feature_ref_offset <- sum_features_respec$feature_ref_offset[[sum_feature_i]]
  before <- sum_features_respec$before[[sum_feature_i]]
  after <- sum_features_respec$after[[sum_feature_i]]
  latest %>%
    select(all_of(c(key_colnames(.), predictor))) %>%
    filter(between(.data$time_value,
                   .env$testing_reference_time + .env$feature_ref_offset - .env$before,
                   .env$testing_reference_time + .env$feature_ref_offset + .env$after)) %>%
    filter(!is.na(.data[[predictor]])) %>%
    transmute(
      ek = .[key_colnames(latest, exclude = "time_value")],
      time_value,
      conf_version = corresponding_confkey_versions(., archive, predictor)
    ) %>%
    mutate(
      ref_offset = .data$time_value - .env$testing_reference_time,
      conf_lag = .data$conf_version - time_get_zero_lag_version(.env$testing_reference_time, archive$time_type, vec_ptype(.data$conf_version)),
      .keep = "unused"
    ) %>%
    # pack(tv = all_of(c("time_value", "version"))) %>%
    # chop("tv") %>%
    pack(tvoffset = c(ref_offset, conf_lag)) %>%
    chop("tvoffset") %>%
    mutate(predictor = .env$predictor, feature = .env$feature) %>%
    {}
}) %>%
  bind_rows() %>%
  nest(agg_feature = c(feature, predictor, tvoffset)) %>%
  # FIXME TODO complete eks
  chop(ek) %>%
  # {
  #   lapply(seq_len(nrow(.)), function(i) {
  #     as.list(.[i,])
  #   })
  # } %>%
  # transmute(
  #   ek = map_chr(ek, function(some_ek_set) {
  #     map(some_ek_set, format) %>%
  #       reduce(paste, sep = " x ") %>%
  #       paste(collapse = ", ")
  #   }),
  #   agg_feature = map_chr(agg_feature, function(some_agg_feature) {
  #     tv_chr <- map_chr(some_agg_feature$tv, function(some_tv) {
  #       paste0("(", some_tv$time_value, ", ", some_tv$version, ")") %>%
  #         paste(collapse = ", ")
  #     })
  #     paste0(some_agg_feature$predictor, " @ ", tv_chr) %>%
  #       paste(collapse = "; ")
  #   })
  # ) %>%
  # pwalk(function(ek, agg_feature) {cat(ek); cat("\n"); cat(gsub("; ", "\n", agg_feature)); cat("\n---\n")}) %>%
  transmute(
    ek = map_chr(ek, function(some_ek_set) {
      map(some_ek_set, format) %>%
        reduce(paste, sep = " x ") %>%
        paste(collapse = ", ")
    }),
    agg_feature = map_chr(agg_feature, function(some_agg_feature) {
      tvoffset_chr <- map_chr(some_agg_feature$tvoffset, function(some_tvoffset) {
        paste0("(", some_tvoffset$ref_offset, ", ", some_tvoffset$conf_lag, ")") %>%
          paste(collapse = " + ")
      })
      paste0(some_agg_feature$predictor, " @ ", tvoffset_chr) %>%
        paste(collapse = "; ")
    })
  ) %>%
  pwalk(function(ek, agg_feature) {cat(ek); cat("\n"); cat(gsub("; ", "\n", agg_feature)); cat("\n---\n")}) %>%
  {}

# It may be more efficient to do some of this with joins, though still
# might need to group by predictor to preserve classes and for confkey
# stuff.

# sum_features_respec %>%
#   filter(predictor == "admissions") %>%
#   mutate(testing_start = testing_reference_time + ref_offset - before,
#          testing_end = testing_reference_time + ref_offset + after) %>%
#   nest_join(latest, join_by(between(y$time_value, x$testing_start, x$testing_end)))





training_tbl <- archive %>%
  epix_target_evaluation_data(target, -target_offset, anchor_versions = unique(.$DT$version)) %>%
  na.omit() %>%
  mutate(reference_time = anchor_version - target_offset)

# TODO intersect with admissions features availability

# TODO selection criteria etc.
