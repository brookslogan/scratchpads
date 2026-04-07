
library(dplyr)
library(tidyr)

case_death_rate_archive

version_get_containing_time_value(Sys.time(), case_death_rate_archive %>% filter(as.POSIXlt(time_value)$wday == 3) %>% set_time_week_end())


# archive <- case_death_rate_archive
archive <- case_death_rate_archive %>% epix_as_of(.$versions_end - 41L, all_versions = TRUE)
nowcast_date <- archive$versions_end
reference_date <- version_get_containing_time_value(nowcast_date, archive)
latest <- archive %>% epix_as_of_latest()
# target_horizon <- -3
target_horizon <- -40

latest %>%
  as_tibble() %>%
  pivot_longer(-c(geo_value, time_value)) %>%
  {}

latest %>%
  filter(time_value >= max(time_value) - as.difftime(6, units = "weeks")) %>%
  corresponding_diffkey_versions(archive, val_var_subset = "case_rate")

test_archive <-
  bind_rows(
    tibble(geo_value = 1, time_value = 1L, version = 6L, var1 =   1, var2 =  11),
    tibble(geo_value = 1, time_value = 1L, version = 7L, var1 =   1, var2 =  11),
    tibble(geo_value = 2, time_value = 1L, version = 6L, var1 = 101, var2 = 111),
    tibble(geo_value = 2, time_value = 2L, version = 7L, var1 =  NA, var2 = 112)
  ) %>%
  as_epi_archive()

test_archive %>%
  epix_diffkeys(val_var_subset = "var1")

test_archive %>%
  epix_diffkeys(val_var_subset = "var2")

test_archive %>%
  epix_as_of_latest() %>%
  mutate(diffkey_version = corresponding_diffkey_versions(., test_archive, val_var_subset = "var1"))

test_archive %>%
  epix_as_of_latest() %>%
  mutate(diffkey_version = corresponding_diffkey_versions(., test_archive, val_var_subset = "var2"))



target_time_value <- reference_date + target_horizon

# predictors_in_window <- latest %>%
#   # FIXME conversions
#   filter(abs(time_value - target_time_value) <= 60) %>%
#   {
#     metadata <- attr(., "metadata")
#     metadata$other_keys <- c(metadata$other_keys, "name")
#     pivot_longer(., val_colnames(.)) %>%
#       reclass(metadata)
#   } %>%
#   filter(!is.na(value)) %>%
#   select(-value) %>%
#   {}

# predictors_in_window %>%
#   group_by(name) %>%
#   group_modify(function(gd, gk) {
#     gd %>%
#       mutate(diffkey_version = corresponding_diffkey_versions(gd, archive, val_var_subset = gk$name))
#   }) %>%
#   ungroup() %>%
#   {}

# feature_offsets_from_target <- c(-14, -7, 0, 7, 14, 21)
# predictor_agg_offsets_from_feature <- -7:0

# feature_offsets_from_target <- c(-14, -7, 0, 7, 14)
# predictor_agg_offsets_from_feature <- -7:0

# feature_offset_breaks <- c(-14, -7, 0, 7, 14) %>%
#   time_delta_standardize(archive$time_type, "fast")

# feature_offsets_from_target <- c(-14, -7, 0, 7, 14)
# predictor_agg_offsets_from_feature <- -3:3

# tibble(
#   latest = latest
# ) %>%
#   mutate(offset = time_delta_standardize(.data$latest$time_value - .env$target_time_value,
#                                          .env$archive$time_type, "fast")) %>%
#   filter(min(feature_offset_breaks) < offset &
#            offset <= max(feature_offset_breaks)) %>%
#   {}

# feature_offsets_from_target <- c(-14, -7, 0, 7, 14)
# predictor_agg_offsets_from_feature <- -7:0
# # vs. n & align?

predictor <- "case_rate"
offset_from_target <- -7
# window_size <- 7
# align <- "right"
before <- 6
after <- 0
# TODO types...

# latest_dtbl <- as.data.table(latest, key = c("time_value", key_colnames(latest, exclude = "time_value")))

# latest_dtbl[list(time_value = target_time_value + offset_from_target), on = "time_value"]
# # ^ is not completing a concern?

# data.table::data.table(g = c(1,1,2), t = c(1:2,1), key = c("t", "g"))[list(t = 1:2), on = "t"]


feature_time_value <- target_time_value + offset_from_target

distinct_epikeys <- latest[key_colnames(latest, exclude = "time_value")] %>%
  vctrs::vec_unique()

contributing_testing_values <-
  latest %>%
  mutate(case_rate = if_else(geo_value == "dc" & time_value == feature_time_value - 6L, NA, case_rate)) %>%
  select(all_of(c(key_colnames(.), predictor))) %>%
  filter(between(.data$time_value,
                 .env$feature_time_value - .env$before,
                 .env$feature_time_value + .env$after)) %>%
  filter(!is.na(.data[[predictor]])) %>%
  # TODO pack first?
  mutate(diffkey_version = corresponding_diffkey_versions(., archive, , predictor)) %>%
  select(-all_of(predictor)) %>%
  # chop(time_value) %>%
  nest(tv = c(time_value, diffkey_version)) %>%
  pack(epikey = all_of(key_colnames(latest, exclude = "time_value"))) %>%
  complete(epikey = distinct_epikeys,
           fill = list(tv = list(tibble(time_value = vec_ptype(latest$time_value),
                                        version = vec_ptype(version_obj(latest)))))) %>%
  chop(epikey) %>%
  rowwise() %>%
  group_map(function(x, y) {
    print(x$epikey)
    print(x$tv)
    print("--")
  }) %>%
  {}


# tibble(geo_value = "as", time_value = as.Date("2021-11-21")) %>%
#   as_epi_df(as_of = archive$versions_end) %>%
#   corresponding_diffkey_versions(archive, , "case_rate")

# archive$versions_end
# archive$DT[geo_value == "as" & time_value == as.Date("2021-11-21"),]

# archive %>%
#   epix_diffkeys() %>%
#   filter(geo_value == "as")


# unique(as_tibble(as.data.frame(archive$DT))[c("geo_value", "time_value")]) %>%
#   mutate(selected_lag_val = extract2_tvoffset(archive, ., "case_rate", 1L, 28L)) %>%
#   na.omit() %>%
#   {}

unique(as_tibble(as.data.frame(archive$DT))[c("geo_value", "time_value")]) %>%
  mutate(selected_lag_val = extract2_version_lag(archive, ., "case_rate", 14L)) %>%
  na.omit() %>%
  {}

archive_cases_dv_subset %>%
  epix_target_evaluation_data("case_rate_7d_av", 0L)




# sum_feature_time_offsets <- list("death_rate" = 7 * -3:3, "case_rate" = 7 * -3:3 - 21)
# # sum_feature_n <- c("death_rate" = 7, "case_rate" = 7)
# # sum_feature_align <- c("death_rate" = "right", "case_rate" = "right")
# sum_feature_before <- 6
# sum_feature_after <- 0

sum_feature_setup <- bind_rows(
  tibble(predictor = "death_rate", base_offset = 0, addit_offset = 7 * -3:3, before = 6, after = 0),
  tibble(predictor = "case_rate", base_offset = -21, addit_offset = 7 * -3:3, before = 6, after = 0),
)

sum_feature_setup %>%
  # mutate(predictor = factor(predictor, unique(predictor))) %>%
  # arrange(predictor, abs(addit_offset)) %>%
  arrange(abs(addit_offset))

archive %>%
  epix_target_evaluation_data("death_rate", 0L)
