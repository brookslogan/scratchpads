
# request instance/ekrtset, sources -> valset?  but
# what about train test distinctions?

# XXX have gone from ekv -> val to ekrt -> val, but really might need
# to be ekrt -> val, as there is an implicit nowcast date version in
# things that seem like ekrt -> val; e.g., latest, semistable, ...;
# neglecting and implicit might produce issues with generality
# (tvoffset) and composability; implicit might produce issues with
# joining with non-version-aware structures (maybe good, maybe quite
# inconvenient); explicit might be wordy.  Or maybe have been
# forgetting the "latest" part in notation which is what turns the
# version to "implicit" even though it's not really. Confkey lookup
# also means "latest" isn't necessarily the nowcast date; need to be
# wary of assumptions. Though confkey + tvoffset may actually be not
# quite the same issue... tvoffset also yields an archive
# conceptually, and has an implicit nowcast date attached... but
# that's still better treatable as just one version tag, not two?;
# tvoffset on ekt can be just ekt+value, and then version is just
# nowcast date.  Confkey attached version can hopefully be limited to
# treatment as a data-informed/fit parameter plus an intermediate
# index, not an index attached to the output.
#
# maybe reference time should be more explicitly thought of as a
# parameter derived from the nowcast date / data?
#
# implicit vs. explicit / the nature of the latest() operation impact
# how tvindexing works... relative versions may not work since version
# is implicit; may be forced to represent as vlag from the
# (already-tlagged/toffsetted-from-reference-time) observation
# time_value.
# - no it doesn't: we can calculate offset from reference time as
#   easily as from tlagged reference time, right?

# may need different types of vars based on whether they have already
# selected vlag, tlag, or not

# may want caching layer that performs logic about set of freshly
# requested vs. re-requested instances.  again, training vs. testing
# probably makes annoying

library(dplyr)
library(tidyr)
library(epiprocess)
library(checkmate)

basic_var <- function(source_name, value_name) {
  source_name
  value_name
  function(request_keys, source_list) {
    source_obj <- source_list[[source_name]]
    request_keys %>%
      left_join(source_obj, by = names(request_keys), relationship = "one-to-one")
  }
}

tvlag_var <- function(source_name, value_name, tlag, vlag) {
  source_name
  value_name
  # ^ for simple nowcaster, maybe want to just go with single-archive
  # input data and just take a value_name here; then later, generalize
  # to a lens?
  tlag
  vlag
  function(request_keys, source_list) {
    # ^ again, source_list -> archive for simple nowcaster to start?
    archive <- source_list[[source_name]]
    assert_class(archive, "epi_archive")
    stop(".........")
  }
}

# not sure about specifying version - shifted_time_value vs. version -
# reference_time.  And seems hard to name in way that distinguishes,
# plus makes clear that version is relative to a time, not some
# version.  Think this is a ekt -> vals type var, so think don't want
# relative to some version; nowcast_version - testing_reference_time
# will be baked into the offset parameters, rather than being carried
# around in ektv sets.

# XXX in nowcasting helpers branch, may have figured out way to
# enforce forcing of factory args, nice class structure, etc., for
# pipeline segments, which might also apply to var factories.

# XXX special column naming vs. role tracking...

    # - Do we need to construct the variable with the training set
    #   available?  Allows setting vtol right there, as well as name
    #   validation, although still need name validation for the test
    #   set...
    #   - Would this not work when making CV pipeline?
    # - Do we need to have a step/segment to fit/select/train the
    #   vtol? But how would we access that parameter?
    #   - Well, for non-pipeline-framework initial draft, can just
    #     externally calculate on training set and feed into
    #     construction... and that was the plan anyway, for this to be
    #     selected with some "latest" helper, and for this to be able
    #     to work with die cut sums, which for "latest" would use test
    #     proxy from train.  For pipeline, will have some way to
    #     access parameters... but seems messy.  And this may blow up
    #     the "variables simpler/cleaner than segments" idea.  Except
    #     part of potential variables plan was some rules related to
    #     reproducibility that could be used for smart per-row
    #     caching, which tie down even more to pre-computing
    #     parameters that depend on multiple rows.
    #
    # Also, what about delta between "last two" versions?  how does
    # vtol approach work / break here?  do we just need pre-processing
    # to a strict no-tol format?

# XXX for transforms, should request_keys be called something else and
# allowed to have extra columns? but then do we need to explicitly
# state which are the request key cols to ensure joins don't go wrong?
# and also need to route resolved dependency col names... perhaps
# cleaner but slower to rely on variable dependencies plus caching? or
# perhaps necessary; the query keys can change, and that would mean
# that column references would need to also chain back to vars for
# recomputation on missing keys, and could lead to unnecessary
# recomputation if don't have dedicated marker for
# not-computed-yet... Except the dependency variable approach also
# needs access to name mapping for better printing, or for var name to
# be stored in var object rather than what seemed like preferred
# approach with it being more external --- or not; was thinking of var
# name aliasing as a separate op and that would just define it as
# another variable with the preferred name stored as the "default"
# name, accessible downstream.

reltv_var <- function(indicator_name, time_rel_rtv, version_rel_rtv, version_tol) {
  assert_string(indicator_name)
  assert_scalar(time_rel_rtv)
  assert_scalar(version_rel_rtv) # XXX call this confkey version rel rtv?
  assert_scalar(version_tol)
  mapping <- function(request_keys, source_data) {
    assert_class(request_keys, "tbl_df")
    assert_class(source_data, "epi_archive")
    assert_names(names(request_keys), permutation.of = key_colnames(source_data, exclude = "version"))
    #
    extract2_tvoffset(source_data, request_keys, indicator_name, time_rel_rtv, version_rel_rtv, version_tol)
  }
  class(mapping) <- c("reltv_var", "var")
  mapping
}

# TODO better terminology... should reltv be tvoffset, and relt
# reserved for something relative to target t?

# XXX do we need var domain function? (..... then perf & impl work
# tension between domain+lookup vs. just computing full
# archive... latter would use different approach, e.g., tshift
# mutating time column vs. doing that, then reversing it and
# performing a join...)

format_with_sign <- function(x, ...) {
  paste0(fifelse(x >= 0, "+", ""), format(x, ...))
}
# TODO 0 --> empty

format.reltv_var <- function(x, ...) {
  e <- environment(x)
  glue::glue("{e$indicator_name}_{{rtv{format_with_sign(e$time_rel_rtv)}}}^(rtv{format_with_sign(e$version_rel_rtv)})")
}

latest_nearby_lags <- function(archive, target_horizon, predictor_name, predictor_center_lag, lag_window_min, lag_window_max) {
  latest <- archive %>% epix_as_of_latest()
  # FIXME types
  min_lag <- - target_horizon + predictor_center_lag + lag_window_min
  max_lag <- - target_horizon + predictor_center_lag + lag_window_max
  # need reference time
  #
  # confkeys stuff, vtol... see playtesting 2
}

# TODO print and default-var-name (default var names? might make
# packed vs unpacked logic complex though) methods for variables?
# multi-col output already needs to have names in object if outputting
# tibble, so if assume multicolunpack as a uniform interface, could
# just put default names in the tibble; this does require downstream
# to be able to override with multinames.  Could also consider similar
# but no unpacking, which also seems to solve some potential naming
# conflict issues, but then that may result in confusion/bugs when
# referring to wrapped single cols.  Or something like mutate
# interface with unpack-only-unnamed-expr-yielding-data-frame-cols?

# TODO lag range variables for more succinct printing of selected variables?

# Should variables have separate functions for training and testing (and evaluation?) fetching?
# - Separate: one path to allowing differing behavior for target
#   variable on train vs test set vs evaluation set (e.g., use
#   semistable if available vs. complain if more stable available
#   vs. use more stable).
#   - but this could also be achieved with target being mapped to
#     different variables in different contexts
# - Separate: one way to allow simulated/predicted/imputed test-time
#   data to be subbed into model trained on real observations (e.g.,
#   in iterative sample models).  Seems would avoid some awkwardness
#   and near-ambiguity/easily-messed-up-notation in conditional
#   notation (if want Y ~ linear_features | partition/weighting-stuff,
#   then Y ~ linear_features | another_feature =
#   another_feature_imputed might imply something else, and actually Y
#   ~ linear_features | another_feature ~
#   another_feature_sample... well no, this has ~ not =, it's fine,
#   and "proper" way of writing, Y ~ linear_features |
#   feature = imputed_feature)
#   - but as weighting stuff would require some sort of function on
#     RHS like Y ~ X | w(var), seems like might be able to require
#     function even for discrete stratification? |
#     stratified(geo_value)? maybe looks bad though
#   - perhaps better counter is that stratification/weighting could be
#     done with steps/factories, also avoiding formula issues
# - Combined: we don't need differing behavior for predictors, so
#   predictors could be characterized more cleanly as one function
# - Combined: it's unclear how many contexts there are... train and
#   test, maybe, but there's also evaluation, and maybe other
#   contexts.  More hassle to write and are we really going to make
#   the exact desired combination of choices desired?

# latest <- archive_cases_dv_subset %>% epix_as_of_latest()
# # ekts <- latest %>% select(all_of(key_colnames(.)))
# var1 <- reltv_var("percent_cli", -as.difftime(7, units = "days"), as.difftime(0, units = "days"), as.difftime(0, units = "days"))
# var2 <- reltv_var("percent_cli", -as.difftime(14, units = "days"), as.difftime(0, units = "days"), as.difftime(0, units = "days"))
# var3 <- reltv_var("percent_cli", -as.difftime(14, units = "days"), as.difftime(7, units = "days"), as.difftime(0, units = "days"))
# latest %>%
#   mutate(v1 = var1(pick(all_of(key_colnames(.))), archive_cases_dv_subset)) %>%
#   mutate(v2 = var2(pick(all_of(key_colnames(.))), archive_cases_dv_subset)) %>%
#   mutate(v3 = var3(pick(all_of(key_colnames(.))), archive_cases_dv_subset)) %>%
#   drop_na(percent_cli) %>%
#   drop_na(v1) %>%
#   as_epi_df() %>%
#   autoplot(c(percent_cli, v1, v2, v3), .color_by = ".response", .facet_by = "all_keys")

# TODO how to track roles?  maybe could have vars output a tibble
# column per role?  But then only basic roles; no role relative to
# another column; should role be held in variable instead?  But then
# does it need to get its actual names?

# For variable setup, looks like will want to feed in the training set
# or a "design(ing) set" to be able to do latest-missingness-informed
# variable selection.  Allowing for separate design set allows for
# that selection to be static if we run it on a burn-in set.  Would
# this also relate to choosing fixed per-location eta for MultiQT?

# Die-cut sum feature... if want to generalize then probably need to
# take different computation approach.  Streaming of entry per window
# not very general.  Maybe form all data into matrix and then use
# apply?  Or is that too much space?  Current epix_slide as well as
# tucked-away faster variant take row/version-streaming approach
# rather than column/tlag-streaming... but unless have each window
# cover multiple features, no re-use, and no reason to be using
# "slide".  Instead, consider chunking requests, and within each
# chunk, using potentially-optimized group-mean/whatever operations.

# TODO better name than var?  could be confused with colname...

# XXX if having design set after all, then should vars all take in the
# design set for easier / more immediate / portable validation?

die_cut_agg_var <- function(indicator_name, times_rel_rtv, versions_rel_rtv, agg_fn, version_tol) {
  assert_string(indicator_name)
  c(times_rel_rtv, versions_rel_rtv) %<-% vctrs::vec_recycle_common(times_rel_rtv, versions_rel_rtv)
  assert_function(agg_fn)
  force(version_tol)
  mapping <- function(request_keys, data_set) {
    assert_class(request_keys, "tbl_df")
    assert_class(data_set, "epi_archive")
    assert_names(names(request_keys), permutation.of = key_colnames(data_set, exclude = "version"))
    times_rel_rtv <- time_delta_standardize(times_rel_rtv, data_set$time_type)
    versions_rel_rtv <- validate_nice_version_lags(versions_rel_rtv, data_set)
    conf_ekvs <- epix_confkeys(data_set, indicator_name)
    version_tol <- vtol_preprocess(version_tol, data_set, conf_ekvs)
    #
    ekv_colnames <- key_colnames(data_set, exclude = "time_value")
    setDT(conf_ekvs, key = ekv_colnames)
    # TODO consider chunking for smaller memory usage
    #
    # translate requests for aggregate into requests for contributing values:
    cross_join(tibble(request_ind = seq_len(nrow(request_keys)),
                      request_key = request_keys),
               tibble(time_rel_rtv = times_rel_rtv, version_rel_rtv = versions_rel_rtv)) %>%
      transmute(
        request_ind,
        subrequest_keys = tibble(
          request_key %>% select(!"time_value"), # unpacks
          time_value = request_key$time_value + time_rel_rtv,
          version = request_key$time_value + version_rel_rtv
        )
      ) %>%
      mutate(
        real_versions_info = conf_ekvs[
          subrequest_keys[ekv_colnames], on = ekv_colnames, roll = "nearest",
          list(real_version = x.version, vdiff = x.version - i.version)
        ]
      ) %>%
      print() %>%
      {
        .$subrequest_keys$version <- .$real_versions_info$real_version
        print(data_set$DT)
        print(.$subrequest_keys)
        subrequest_results <- data_set$DT[
          .$subrequest_keys, on = key_colnames(data_set), roll = TRUE,
          indicator_name,
          with = FALSE
        ][[indicator_name]] # `with = FALSE` -> have to manually extract2
        if (version_tol$inclusive) {
          subrequest_results[.$real_versions_info[, abs(vdiff) > version_tol$threshold]] <- NA
        } else {
          subrequest_results[.$real_versions_info[, abs(vdiff) >= version_tol$threshold]] <- NA
        }
        .$subrequest_result <- subrequest_results
        .
      } %>%
      # TODO make above into an extract2_tvoffsets function?
      summarize(.by = request_ind, result = agg_fn(subrequest_result)) %>%
      .$result
  }
  class(mapping) <- c("die_cut_agg_var", "var")
  mapping
}

# die_cut_agg_var <- function(indicator_name, times_rel_rtv, versions_rel_rtv, agg_fn, version_tol) {

# seq_encoding <- function(x, ...) {
#   UseMethod("seq_encoding")
# }

# seq_encoding.numeric <- function(x, ...) {
#   x_diff <- diff(x)
#   x_diff_rle
#   stop("TODO")
# }

# format.die_cut_agg_var <- function(x, ...) {
#   e <- environment(x)
#   tvoffsets <- tibble(time_rel_rtv = times_rel_rtv, version_rel_rtv = versions_rel_rtv)
#   range_toffset <- range(tvoffsets$time_rel_rtv)
#   min_dtoffset <- min(diff(tvoffsets$time_rel_rtv))
#   if (all(time_rel_rtv %in% seq(range_toffset[[1L]], range_toffset[[2L]], by = min_dtoffset))) {
#     # ^ %in% seq: trying to work around classes without %% or simple as.numeric
#     stop("TODO various stuff here and around")
#   }
#   i <- 1L
#   while(i <= nrow(tvoffsets)) {
#     if (tvoffsets[[i + 1L, "version_rel_rtv"]] == tvoffsets[[i, "version_rel_rtv"]]) {
#       dtoffset <- tvoffsets[[i + 1L, "time_rel_rtv"]] - tvoffsets[[i, "time_rel_rtv"]]
#     }
#   }
# }

# TODO stringification methods

zero_date <- as.Date("2020-01-01") - 1

var <- die_cut_agg_var("value", c(-2, 0), 1, toString, version_tol = 1)

var(
  tibble(geo_value = 1, time_value = zero_date + c(2, 3)),
  as_epi_archive(tibble(
    geo_value = 1,
    time_value = zero_date + c(1, 2, 3),
    version = zero_date + c(2, 3, 4),
    value = c(1:3)
  ))
)

var(
  tibble(geo_value = 1, time_value = zero_date + c(2, 3)),
  as_epi_archive(tibble(
    geo_value = 1,
    time_value = zero_date + c(1, 2, 3),
    version = zero_date + c(2, 5, 5),
    value = c(1:3)
  ))
)

var(
  tibble(geo_value = 1, time_value = zero_date + c(2, 3)),
  as_epi_archive(tibble(
    geo_value = 1,
    time_value = zero_date + c(1, 2, 3),
    version = zero_date + c(5, 3, 4),
    value = c(1:3)
  ))
)

# TODO how to name? want some compression of toffsets and voffsets,
# but function unless one of a few known may need to have a user name
# put in... and even then, the auto name may be pretty bad if
# expressing t and v offset range rather than concept like rolling
# average... and compressing set to range requires some amount of
# knowledge of natural spacing...

# TODO enforce size 1 outputs... or rely on summarize() doing that for
# us?  when was multi-row-out hard-deprecated?
