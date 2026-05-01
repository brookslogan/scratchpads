
library(dplyr)
library(tidyr)
library(purrr)
library(epidatr)
library(vctrs)

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
  epix_as_of(nowcast_version, all_versions = TRUE) %>%
  filter(geo_value == "ak")

time_type <- archive$time_type
testing_reference_time <- version_get_containing_time_value(nowcast_version, archive)
latest <- archive %>% epix_as_of_latest()


target <- "admissions"
target_horizon <- 7

# TODO need to ensure nice input offsets & standardize... or a window spec?
sum_feature_specs <- bind_rows(
  tibble(predictor = "admissions", offset = 7 * -8:8, align = "right", window_size = 7),
  tibble(predictor = "google", offset = 7 * -8:8, align = "right", window_size = 1)
) %>%
  mutate(horizon = target_horizon + offset) %>%
  mutate(before = case_when( # case_match deprecated
    align == "right" ~ window_size - 1L,
    align == "center" ~ floor(window_size / 2),
    align == "left" ~ 0L
  )) %>%
  mutate(after = window_size - before - 1L) %>%
  mutate(feature = glue::glue_data(., '{predictor}_{window_size}{time_type_unit_abbr(time_type)}{c(right = "", center = "c", left = "l")[align]}sum_{abs(offset)}{time_type_unit_abbr(time_type)}{if_else(offset <= 0, "lag", "lead")}'))
# TODO 1dsum -> 1d?

available_sum_feature_templates <- lapply(seq_len(nrow(sum_feature_specs)), function(sum_feature_i) {
  feature <- sum_feature_specs$feature[[sum_feature_i]]
  predictor <- sum_feature_specs$predictor[[sum_feature_i]]
  horizon <- sum_feature_specs$horizon[[sum_feature_i]]
  before <- sum_feature_specs$before[[sum_feature_i]]
  after <- sum_feature_specs$after[[sum_feature_i]]
  latest %>%
    select(all_of(c(key_colnames(.), predictor))) %>%
    filter(between(.data$time_value,
                   .env$testing_reference_time + .env$horizon - .env$before,
                   .env$testing_reference_time + .env$horizon + .env$after)) %>%
    filter(!vec_detect_missing(.data[[predictor]])) %>%
    transmute(
      time_value,
      conf_version = corresponding_confkey_versions(., archive, predictor)
    ) %>%
    mutate(
      summand_horizon = .data$time_value - .env$testing_reference_time,
      summand_conf_lag = .data$conf_version - time_get_zero_lag_version(.env$testing_reference_time, archive$time_type, vec_ptype(.data$conf_version)),
      .keep = "unused"
    ) %>%
    pack(tv_template = c(summand_horizon, summand_conf_lag)) %>%
    chop("tv_template") %>%
    mutate(predictor = .env$predictor, feature = .env$feature) %>%
    {}
}) %>%
  bind_rows()

predictor_vtols <- unique(sum_feature_specs$predictor) %>%
  `names<-`({.}) %>%
  lapply(function(predictor) vtol_preprocess(NULL, archive, epix_confkeys(archive, predictor)))

extract2_template_sum_feature <- function(archive, ekts, template_sum_feature, vtol) {
  stopifnot(nrow(template_sum_feature) == 1L)
  predictor <- template_sum_feature$predictor
  tv_templates <- template_sum_feature$tv_template[[1L]] # unwrap
  stopifnot(nrow(tv_templates) > 0L)
  for (tv_template_i in seq_len(nrow(tv_templates))) {
    # ^ maybe keep memory in check vs. map-reduce when summing across many offsets
    curr_summand_horizon <- vec_slice(tv_templates$summand_horizon, tv_template_i)
    curr_summand_conf_lag <- vec_slice(tv_templates$summand_conf_lag, tv_template_i)
    # TODO find names to distinguish target-relative and reference-relative offsets
    contrib <- extract2_tvoffset(archive, ekts, predictor, curr_summand_horizon, curr_summand_conf_lag, vtol)
    if (tv_template_i == 1L) {
      result <- contrib
    } else {
      result <- result + contrib
    }
  }
  result
}

training_tbl <- archive %>%
  epix_target_evaluation_data(target, -target_horizon, anchor_versions = unique(.$DT$version)) %>%
  na.omit() %>%
  # mutate(ref_time_value = version_get_containing_time_value(anchor_version, archive), .keep = "unused") %>%
  mutate(time_value = version_get_containing_time_value(anchor_version, archive), .keep = "unused") %>%
  {}
actual_target <- vctrs::vec_set_difference(names(training_tbl), key_colnames(latest))
for (i in seq_len(nrow(available_sum_feature_templates))) {
  feature <- available_sum_feature_templates[i,]
  vtol <- predictor_vtols[[available_sum_feature_templates$predictor[[i]]]]
  training_tbl <- training_tbl %>%
    mutate_new(!!available_sum_feature_templates$feature[[i]] :=
                 # TODO require ref_time[_value] in this function rather than time_value?
                 extract2_template_sum_feature(archive, training_tbl, feature, vtol))
}
#
target_based_available_sum_feature_templates <- available_sum_feature_templates$feature[
  available_sum_feature_templates$predictor == target
]
# Require training instances to have at least one non-NA target-based
# feature.  Otherwise, if we have a large target data dump without
# real-time target lag data, these features will appear to have large
# amounts of missingness and may lead us to (a) throw out all
# target-based features (if we have auxiliary predictor features with
# real-time data that make these instances look usable), or (b) refuse
# to proceed (otherwise).
training_tbl <- training_tbl %>%
  filter(!if_all(all_of(target_based_available_sum_feature_templates), vec_detect_missing))

min_instance_feature_ratio <- 2
min_n_instances <- 15L
max_n_times <- Inf

seq_len(nrow(available_sum_feature_templates)) %>%
  `names<-`(available_sum_feature_templates$feature) %>%
  vapply(function(available_sum_feature_i) {
    feature <- available_sum_feature_templates$feature[[available_sum_feature_i]]
    simple_fit_tbl <- training_tbl %>%
      .[c(key_colnames(latest), feature, actual_target)] %>%
      transmute(ek = pick(all_of(key_colnames(latest, exclude = "time_value"))),
                time_value,
                feat = .data[[feature]],
                targ = .data[[actual_target]]) %>%
      drop_na(feat, targ) %>%
      # NOTE: assumes only one epikey
      slice_min(abs(.data$time_value - .env$testing_reference_time), n = max_n_times) %>%
      mutate(intercept = 1)
    if (nrow(simple_fit_tbl) < min(min_instance_feature_ratio * 2L, min_n_instances)) {
      NA_real_
    } else {
      # TODO constant / all-zero detection?
      fit <- quantreg::rq.fit(as.matrix(simple_fit_tbl[c("feat", "intercept")]), simple_fit_tbl[["targ"]], method = "fn")
      mean(abs(fit$residuals))
    }
  }, numeric(1)) %>%
  sort()
# XXX this was meant to be just something simple and naive, not
# requiring looking at joint intersections or otherwise looking at
# missingness, but it seems this first test case is an example of
# where things go wrong.  Instead of requiring joint intersections,
# perhaps we can just assume the worst where missing?

# TODO exclude intercept-redundant features?
training_tbl <- training_tbl %>%
  mutate(intercept = 1)
all_features <- c(available_sum_feature_templates$feature, "intercept")

# XXX terminology
#
# feature requests?... predictor requests?  feature queries? asks? candidate features (ambig)? specification? description?
#
# skeleton? armature? roughout?
#
# die cut? pattern sum? partial sum? template sum? mirror? silhouette? cookie cutter? mask?
#
# blank? blueprint? outline? tailor? schematic?
#
# form? contour? mold? template?
#
# analog? shadow?
#
# sketch... underpaint... block in... detail... finish

training_tbl %>%
  as_epi_df() %>%
  mutate(across(where(is.numeric), ~ .x / mean(.x, na.rm = TRUE))) %>%
  autoplot(where(is.numeric), .facet_by = "geo_value", .color_by = ".response") %>%
  `+`(ggplot2::ggtitle("SEOIFJ")) %>%
  `+`(ggplot2::xlab("blah")) %>%
  `+`(ggplot2::ylab("blah")) %>%
  plotly::ggplotly()
