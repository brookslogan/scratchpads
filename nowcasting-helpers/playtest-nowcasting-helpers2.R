
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

tasksets_candidate_features <- lapply(seq_len(nrow(sum_features_respec)), function(sum_feature_i) {
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
  nest(die_cut_sum_feature = c(feature, predictor, tvoffset)) %>%
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
  #   die_cut_sum_feature = map_chr(die_cut_sum_feature, function(some_die_cut_sum_feature) {
  #     tv_chr <- map_chr(some_die_cut_sum_feature$tv, function(some_tv) {
  #       paste0("(", some_tv$time_value, ", ", some_tv$version, ")") %>%
  #         paste(collapse = ", ")
  #     })
  #     paste0(some_die_cut_sum_feature$predictor, " @ ", tv_chr) %>%
  #       paste(collapse = "; ")
  #   })
  # ) %>%
  # pwalk(function(ek, die_cut_sum_feature) {cat(ek); cat("\n"); cat(gsub("; ", "\n", die_cut_sum_feature)); cat("\n---\n")}) %>%
  # transmute(
  #   ek = map_chr(ek, function(some_ek_set) {
  #     map(some_ek_set, format) %>%
  #       reduce(paste, sep = " x ") %>%
  #       paste(collapse = ", ")
  #   }),
  #   die_cut_sum_feature = map_chr(die_cut_sum_feature, function(some_die_cut_sum_feature) {
  #     tvoffset_chr <- map_chr(some_die_cut_sum_feature$tvoffset, function(some_tvoffset) {
  #       paste0("(", some_tvoffset$ref_offset, ", ", some_tvoffset$conf_lag, ")") %>%
  #         paste(collapse = " + ")
  #     })
  #     paste0(some_die_cut_sum_feature$predictor, " @ ", tvoffset_chr) %>%
  #       paste(collapse = "; ")
  #   })
  # ) %>%
  # pwalk(function(ek, die_cut_sum_feature) {cat(ek); cat("\n"); cat(gsub("; ", "\n", die_cut_sum_feature)); cat("\n---\n")}) %>%
  {}

# It may be more efficient to do some of this with joins, though still
# might need to group by predictor to preserve classes and for confkey
# stuff.

# sum_features_respec %>%
#   filter(predictor == "admissions") %>%
#   mutate(testing_start = testing_reference_time + ref_offset - before,
#          testing_end = testing_reference_time + ref_offset + after) %>%
#   nest_join(latest, join_by(between(y$time_value, x$testing_start, x$testing_end)))


predictor_vtols <- unique(candidate_features$predictor) %>%
  `names<-`({.}) %>%
  lapply(function(predictor) vtol_preprocess(NULL, archive, epix_confkeys(archive, predictor)))

taskset_i <- 1L
taskset <- tasksets_candidate_features$ek[[taskset_i]]
candidate_features <- tasksets_candidate_features$die_cut_sum_feature[[taskset_i]]

extract2_die_cut_sum_feature <- function(archive, ekts, die_cut_sum_feature, vtol) {
  stopifnot(nrow(die_cut_sum_feature) == 1L)
  predictor <- die_cut_sum_feature$predictor
  tvoffsets <- die_cut_sum_feature$tvoffset[[1L]] # unwrap
  stopifnot(nrow(tvoffsets) > 0L)
  for (tvoffset_i in seq_len(nrow(tvoffsets))) {
    # ^ maybe keep memory in check vs. map-reduce when summing across many offsets
    ref_offset <- vec_slice(tvoffsets$ref_offset, tvoffset_i)
    conf_lag <- vec_slice(tvoffsets$conf_lag, tvoffset_i)
    contrib <- extract2_tvoffset(archive, ekts, predictor, ref_offset, conf_lag, vtol)
    if (tvoffset_i == 1L) {
      result <- contrib
    } else {
      result <- result + contrib
    }
  }
  result
}

# training_tbl <- archive %>%
#   epix_target_evaluation_data(target, -target_offset, anchor_versions = unique(.$DT$version)) %>%
#   na.omit() %>%
#   mutate(time_value = anchor_version - target_offset, .keep = "unused") %>%
#   {}

# extract2_tvoffset(archive, training_tbl, "google", -7, as.difftime(0, units = "days"))

# extract2_die_cut_sum_feature(archive, training_tbl, candidate_features[1L,], predictor_vtols[[candidate_features$predictor[[1L]]]])

# extract2_die_cut_sum_feature(archive, training_tbl, candidate_features[2L,], predictor_vtols[[candidate_features$predictor[[2L]]]])

# training_tbl %>%
#   mutate(val = extract2_die_cut_sum_feature(archive, training_tbl, candidate_features[3L,], predictor_vtols[[candidate_features$predictor[[3L]]]])) %>%
#   count(time_value, is.na(val)) %>%
#   print(n = 2000)

# extract2_die_cut_sum_feature(archive, training_tbl, candidate_features[2L,], predictor_vtols[[candidate_features$predictor[[2L]]]])


training_tbl <- archive %>%
  epix_target_evaluation_data(target, -target_offset, anchor_versions = unique(.$DT$version)) %>%
  na.omit() %>%
  mutate(time_value = anchor_version - target_offset, .keep = "unused") %>%
  {}
for (i in seq_len(nrow(candidate_features))) {
  feature <- candidate_features[i,]
  vtol <- predictor_vtols[[candidate_features$predictor[[i]]]]
  training_tbl <- training_tbl %>%
    mutate_new(!!candidate_features$feature[[i]] :=
                 extract2_die_cut_sum_feature(archive, training_tbl, feature, vtol))
}
target_based_candidate_features <- candidate_features$feature[
  candidate_features$predictor == target
]
# Require training instances to have at least one non-NA target-based
# feature.  Otherwise, if we have a large target data dump without
# real-time target lag data, these features will appear to have large
# amounts of missingness and may lead us to (a) throw out all
# target-based features (if we have auxiliary predictor features with
# real-time data that make these instances look usable), or (b) refuse
# to proceed (otherwise).
training_tbl <- training_tbl %>%
  filter(!if_all(all_of(target_based_candidate_features), vec_detect_missing))

# check_feature_suitable <- function(training_tbl, training_feature_col) {
#   # min_time_covered <- as.difftime(90, units = "days")
#   # min_n_instances <-
#   # min_instance_feature_ratio <-
# }

# check_training_tbl_suitable <- function(training_tbl) {
#   min_time_covered <- as.difftime(90, units = "days")
#   min_n_instances <-
#   min_instance_feature_ratio <-
# }

# TODO need to require consideration of at least one of these features to begin with

# TODO some sort to try to prioritize target-based features a bit

# FIXME TODO still need to check we didn't drop anything in tasksets

# TODO intersect with admissions features availability

# TODO selection criteria etc.
