library(epidatr)
library(epiprocess)
library(dplyr)
library(tidyr)
library(data.table)
library(purrr)
library(vctrs)

cce <- covidcast_epidata()

tn_issues <- cce$signals$"hospital-admissions:smoothed_covid19"$call("state", "tn", "*", issues = "*")
ne_issues <- cce$signals$"hospital-admissions:smoothed_covid19"$call("state", "ne", "*", issues = "*")

tn_archive <- tn_issues %>%
  select(geo_value, time_value, version = issue, value) %>%
  as_epi_archive(compactify = TRUE)
ne_archive <- ne_issues %>%
  select(geo_value, time_value, version = issue, value) %>%
  as_epi_archive(compactify = TRUE)
two_archive <- bind_rows(tn_issues, ne_issues) %>%
  select(geo_value, time_value, version = issue, value) %>%
  as_epi_archive(compactify = TRUE)

two_archive2 <-
  two_archive %>%
  epix_slide(~ .x %>% epi_slide_sum(.window_size = 7L, value)) %>%
  as_epi_archive(other_keys = two_archive$other_keys,
                 clobberable_versions_start = two_archive$clobberable_versions_start,
                 versions_end = two_archive$versions_end,
                 compactify = TRUE)

## two_archive2p <-


other_keys <- two_archive$other_keys
two_archive$DT[, list(min(time_value), max(time_value)), by = c("geo_value", other_keys, "version")]




archive <- two_archive


archive <- tibble::tibble(
  geo_value = 1,
  time_value = c(1:7, 3, 10),
  version = c(rep(1, 7), 2, 2),
  value = c(rep(1, 7), 8, 5)
) %>%
  as_epi_archive()

archive_with_7dsum <-
  archive %>%
  epix_slide(function(edf, group_key, version) {
    edf %>%
      epi_slide_sum(.window_size = 7L, value, .new_col_names = "value_7dsum")
  }) %>%
  as_epi_archive(other_keys = archive$other_keys,
                 clobberable_versions_start = archive$clobberable_versions_start,
                 versions_end = archive$versions_end,
                 compactify = TRUE)



## archive_with_7dsum <-
##   archive %>%
##   epix_slide(
##     ~ .x %>%
##                group_by(geo_value, !!!attr(.x, "metadata")$other_keys) %>%
##                epi_slide_sum(.window_size = 7L, value) %>%
##                ungroup() %>%
##                rename(value_7dsum = slide_value_value)) %>%
##   as_epi_archive(other_keys = archive$other_keys,
##                  clobberable_versions_start = archive$clobberable_versions_start,
##                  versions_end = archive$versions_end,
##                  compactify = TRUE)


## copy(archive$DT)[, time_value2 := time_value - as.POSIXlt(time_value)$wday][]

## DT = copy(archive$DT)[, time_value2 := time_value - (time_value - 1L) %% 7L]
## (
##   DT
##   ## [, list(versions = list(sort(unique(version)))), by = c("geo_value", "time_value2")]
##   [, list(version = sort(unique(version))), by = c("geo_value", "time_value2")]
##   [DT[, version := NULL], on = c("geo_value", "time_value2"), allow.cartesian = TRUE]
## )




## DT = copy(archive$DT)[, time_value2 := time_value - (time_value - 1L) %% 7L]






DT = copy(two_archive$DT)
bench::mark(
  DT[, list(data = list(.SD)), keyby = "version"],
  nest(DT, data = -version) %>% setkeyv("version"),
  nest_by(DT, version) |> ungroup(),
  check = FALSE # at least some inner keying differences
)

bench::mark(
{
  DT = copy(two_archive$DT)
  epikeytime_colnames <- key(DT)[key(DT) != "version"]
  # ascending versions
  nested <- DT[, list(data = list(.SD)), keyby = "version"]
  snapshot_dtbl <- DT[integer(0)][, version := NULL][] # (keyed by epikeytime_colnames)
  res1 <- lapply(seq_len(nrow(nested)), function(version_i) {
    version <- nested$version[[version_i]]
    update_dtbl <- nested$data[[version_i]] # (keyed by epikeytime_colnames)
    snapshot_dtbl <<- unique(rbind(snapshot_dtbl, update_dtbl), by = epikeytime_colnames, fromLast = TRUE)
    setkeyv(snapshot_dtbl, epikeytime_colnames)
    new_epi_df(as_tibble(as.data.frame(snapshot_dtbl)), geo_type = two_archive$geo_type, time_type = two_archive$time_type, as_of = version, other_keys = two_archive$other_keys)
  })
}
,
{
  DT = copy(two_archive$DT)
  epikeytime_colnames <- key(DT)[key(DT) != "version"]
  # ascending versions
  nested <- DT[, list(data = list(.SD)), keyby = "version"]
  snapshot_dtbl <- DT[integer(0)][, version := NULL][] # (keyed by epikeytime_colnames)
  res2 <- lapply(seq_len(nrow(nested)), function(version_i) {
    version <- nested$version[[version_i]]
    update_dtbl <- nested$data[[version_i]] # (keyed by epikeytime_colnames)
    snapshot_dtbl <<- unique(rbind(snapshot_dtbl, update_dtbl), by = epikeytime_colnames, fromLast = TRUE)
    new_epi_df(as_tibble(as.data.frame(snapshot_dtbl)), geo_type = two_archive$geo_type, time_type = two_archive$time_type, as_of = version, other_keys = two_archive$other_keys) %>%
      arrange_canonical()
  })
}
## , check = FALSE
)

waldo::compare(res1[[5]], res2[[5]])

waldo::compare(res1[[5]], two_archive %>% epix_as_of(as.Date('2020-07-04')) %>% as.data.table() %>% as.data.frame() %>% as_epi_df())

waldo::compare(res2[[5]], two_archive %>% epix_as_of(as.Date('2020-07-04')) %>% as.data.table() %>% as.data.frame() %>% as_epi_df())


system.time({
  value_nm <- "value"
  out_nm <- "value_7dsum"
  DT = copy(two_archive$DT)
  epikeytime_colnames <- key(DT)[key(DT) != "version"]
  epikey_colnames <- epikeytime_colnames[epikeytime_colnames != "time_value"]
  # ascending versions
  nested <- DT[, list(data = list(.SD)), keyby = "version"]
  snapshot_dtbl <- DT[integer(0)][, version := NULL][] # (keyed by epikeytime_colnames)
  res <- lapply(seq_len(nrow(nested)), function(version_i) {
    version <- nested$version[[version_i]]
    update_dtbl <- nested$data[[version_i]] # (keyed by epikeytime_colnames)
    snapshot_dtbl <<- unique(rbind(snapshot_dtbl, update_dtbl), by = epikeytime_colnames, fromLast = TRUE)
    setkeyv(snapshot_dtbl, epikeytime_colnames)
    # TODO try setDF for this conversion and then setDT after computations done?
    edf <- new_epi_df(as_tibble(as.data.frame(snapshot_dtbl)), geo_type = two_archive$geo_type, time_type = two_archive$time_type, as_of = version, other_keys = two_archive$other_keys)
    # TODO try windowing edf based on update range, either here or in
    # epi_slide_opt? More freedom in former since can potentially vary by geo.
    #
    # TODO try a DT-based approach? or dtplyr?
    edf %>%
      group_by(across(all_of(epikey_colnames))) %>%
      epi_slide_sum(.window_size = 7, all_of(value_nm)) %>%
      rename(!! out_nm := paste0("slide_value_", value_nm)) %>%
      {.}
    # TODO efficient recompactification
  })
})

## profvis::profvis(
## replicate(50, {
##   ## DT = copy(two_archive$DT)
##   ## epikeytime_colnames <- key(DT)[key(DT) != "version"]
##   ## # ascending versions
##   ## nested <- DT[, list(data = list(.SD)), keyby = "version"]
##   ## snapshot_dtbl <- DT[integer(0)][, version := NULL][] # (keyed by epikeytime_colnames)
##   ## res2 <- lapply(seq_len(nrow(nested)), function(version_i) {
##   ##   version <- nested$version[[version_i]]
##   ##   update_dtbl <- nested$data[[version_i]] # (keyed by epikeytime_colnames)
##   ##   snapshot_dtbl <- unique(rbind(snapshot_dtbl, update_dtbl), by = epikeytime_colnames, fromLast = TRUE)
##   ##   as_epi_df(as.data.frame(snapshot_dtbl), as_of = version)
##   ## })
##   as_epi_df(asdf)
##   42
## })
## )

## bench::mark(as_epi_df(asdf), min_time = 20)

## reprex::reprex({
##   library(dplyr)
##   library(epiprocess)

  dup_check1 <- function(x, other_keys) {
    duplicated_time_values <- x %>%
      group_by(across(all_of(c("geo_value", "time_value", other_keys)))) %>%
      filter(dplyr::n() > 1) %>%
      ungroup()
    nrow(duplicated_time_values) != 0
  }

  dup_check2 <- function(x, other_keys) {
    anyDuplicated(x[c("geo_value", "time_value", other_keys)]) != 0L
  }

  dup_check3 <- function(x, other_keys) {
    if (nrow(x) <= 1L) {
      FALSE
    } else {
      epikeytime_names <- c("geo_value", "time_value", other_keys)
      arranged <- arrange(x, across(all_of(epikeytime_names)))
      arranged_epikeytimes <- arranged[epikeytime_names]
      any(vctrs::vec_equal(arranged_epikeytimes[-1L,], arranged_epikeytimes[-nrow(arranged_epikeytimes),]))
    }
  }

##   test_tbl <- as_tibble(covid_case_death_rates_extended)

##   bench::mark(
##     dup_check1(test_tbl, character()),
##     dup_check2(test_tbl, character()),
##     dup_check3(test_tbl, character())
##   )
## })

## dt1 = data.table(k = 1:2, v = 1:2, key = "k")
## dt2 = data.table(k = 2:3, v = 12:13, key = "k")
dt1 = nested$data[[5]]
dt2 = nested$data[[6]]

update1 <- function(dt1, dt2) {
  dt1[dt1[dt2, on = key(dt1), which = TRUE]] <- dt2 # seems to be vanilla; doesn't mutate, destroys key
  dt1 <- rbind(dt1, dt2[!dt1, on = key(dt2)])
  setkeyv(dt1, key(dt2))[]
}

update2 <- function(dt1, dt2) {
  setkeyv(unique(rbind(dt2, dt1), by = key(dt1)), key(dt1))[]
}

update2b <- function(dt1, dt2) {
  setkeyv(unique(rbind(dt1, dt2), by = key(dt1), fromLast = TRUE), key(dt1))[]
}

# TODO update window ranges...

## |      .           ...   .  .   .   1d changes
## |      ...         ................ 3d potential changes
## |    .....       .................. 1d requests


## |      .           ...   .  .   .    1d changes
## |    ....        .................   1d,2d potential changes
## |   .......     .................... 1d requests


## |              .        1d changes
## |             ........  b6d,a1d potential changes ignoring availability
## |       ............... 1d requests
## |       . .  ..... . .. 1d possible availability pattern
## |             .... . .  b6d,a1d potential actual slide outputs?

## bench::mark(
##   # XXX careful to add back copying if needed
##   update1(dt1, dt2),
##   update2(dt1, dt2),
##   update2b(dt1, dt2)
## )

## dt1[dt1[dt2, on = "k", which = TRUE]] <- dt2
## # FIXME additions

## dt1[dt2, on = "k"]
## dt1[dt2, list(k, v), on = "k"]
## dt1[dt2, .SD, on = "k"]
## dt1[!dt2, on = "k"]


## dt1[dt2, .SD, by = .EACHI]
## dt2[dt1, .SD, by = .EACHI]
## dt1[dt2, list(v), by = .EACHI]
## dt2[dt1, list(v), by = .EACHI]


## dt2[dt1, on = "k"]
## dt2[dt1, .SD, on = "k"]
## dt2[dt1, list(k, v), on = "k"]

## dt1
## dt2[dt1, on = "k", nomatch = NULL]
## dt2[!dt1, on = "k"]

## something based on
## merge(all = TRUE)?

## map_ea <- function(.x, .f, ..., .progress = FALSE) {
## }

snapshot_loader_as_epi_archive <- function(versions, snapshot_loader, clobberable_versions_start = NA, compactify_tol = .Machine$double.eps^0.5) {
  if (length(versions) == 0L) {
    cli_abort("`versions` must have positive length")
  }
  # XXX or validate?:
  versions <- sort(unique(versions))

  other_keys <- NULL
  previous_snapshot <- NULL
  diffs <- lapply(versions, function(version) {
    snapshot <- snapshot_loader(version)
    # TODO refactor to use key_colnames and/or as_epi_df
    if (is_epi_df(snapshot)) {
      snapshot_other_keys <- attr(snapshot, "metadata")[["other_keys"]]
      snapshot <- as_tibble(snapshot)
      # FIXME think about aliasing
      setDT(snapshot)
    } else if (is.data.table(snapshot)) {
      if (!all(c("geo_value", "time_value") %in% names(snapshot))) {
        cli_abort("Snapshots of class data.table must contain")
      }
      snapshot_other_keys <- setdiff(key(snapshot), c("geo_value", "time_value", "version"))
    } else {
      cli_abort("`snapshot_loader` must produce an `epi_df` or `data.table`")
    }

    if (is.null(other_keys)) {
      other_keys <<- snapshot_other_keys
    } else if (!identical(other_keys, snapshot_other_keys)) {
      cli_abort("Inconsistent key settings; `other_keys` seemed to be
                 `c({epiprocess:::format_chr_with_quotes(other_keys)})` but then later
                 `c({epiprocess:::format_chr_with_quotes(snapshot_other_keys)})`.")
    }

    snapshot[, version := ..version]

    if (is.null(previous_snapshot)) {
      diff <- snapshot
    } else {
      # diff <- setdiff(snapshot, previous_snapshot)
      diff <- rbind(previous_snapshot, snapshot)
      setkeyv(diff, c("geo_value", other_keys, "time_value", "version"))
      diff <- diff[!Reduce(`&`, lapply(diff[, !"version"], epiprocess:::is_locf, abs_tol = compactify_tol))]
      # `i` arg doesn't support ..var syntax; fake it:
      ..version <- version
      diff <- diff[version == ..version]
    }

    previous_snapshot <<- snapshot

    diff
  })

  diffs <- rbindlist(diffs)
  setkeyv(diffs, c("geo_value", other_keys, "time_value", "version"))
  setcolorder(diffs) # default: keys first

  as_epi_archive(
    diffs, other_keys = other_keys, clobberable_versions_start = clobberable_versions_start, versions_end = max(versions),
    compactify = FALSE # we already compactified; don't re-do work or change tol
  )
}

# XXX but this is temporarily constructing the full snapshot... do we need
# something that allows update loaders rather than snapshot loaders?



snapshots <-
  epidatasets::archive_cases_dv_subset %>%
  epix_slide(~ list(.x))

fewer_snapshots <- head(snapshots, n = 200)

## bounded_snapshots <-
##   epidatasets::archive_cases_dv_subset %>%
##   epix_slide(~ list(.x), .before = 60)

alt_snapshots <-
  epidatasets::case_death_rate_archive %>%
  epix_slide(~ list(.x))

alt_fewer_snapshots <- head(alt_snapshots, n = 200)

## alt_bounded_snapshots <-
##   epidatasets::case_death_rate_archive %>%
##   epix_slide(~ list(.x), .before = 60)


## snapshots <-
##   as_epi_archive(tibble(
##     geo_value = 1, time_value = 1, version = c(1,2,3), value = c(1,1,2)
##   ), compactify = FALSE) %>%
##   epix_slide(~ list(.x))


larger_snapshots <-
  epidatasets::archive_cases_dv_subset_all_states %>%
  epix_slide(~ list(.x))

larger_fewer_snapshots <- head(larger_snapshots, n = 200)

## larger_bounded_snapshots <-
##   epidatasets::archive_cases_dv_subset_all_states %>%
##   epix_slide(~ list(.x), .before = 60)


reconstruction <-
  snapshots$version %>%
  snapshot_loader_as_epi_archive(function(version) {
    snapshots$slide_value[[match(version, snapshots$version)]]
  })

## identical(epidatasets::archive_cases_dv_subset, reconstruction) # original compactified w/o tol

identical(epidatasets::archive_cases_dv_subset$DT %>% as_epi_archive(compactify = TRUE), reconstruction)
waldo::compare(epidatasets::archive_cases_dv_subset$DT %>% as_epi_archive(compactify = TRUE), reconstruction)

## is_locf2 <- function(vec, abs_tol) {
##   lag_vec <- dplyr::lag(vec)
##   if (typeof(vec) == "double") {
##     res <-
##       !is.na(vec) & !is.na(lag_vec) & near(vec, lag_vec, tol = abs_tol) |
##       is.na(vec) & is.na(lag_vec)
##     return(res)
##   } else {
##     res <- if_else(
##       !is.na(vec) & !is.na(lag_vec),
##       vec == lag_vec,
##       is.na(vec) & is.na(lag_vec)
##     )
##     return(res)
##   }
## }

## is_locf3 <- function(vec, abs_tol) {
##   lag_vec <- dplyr::lag(vec)
##   if (typeof(vec) == "double") {
##     res <-
##       !is.na(vec) & !is.na(lag_vec) & near(vec, lag_vec, tol = abs_tol) |
##       is.na(vec) & is.na(lag_vec)
##     return(res)
##   } else {
##     res <- if_else(
##       !is.na(vec) & !is.na(lag_vec),
##       vec == lag_vec,
##       is.na(vec) & is.na(lag_vec)
##     )
##     return(res)
##   }
## }

## is_locf4 <- function(vec, abs_tol) { # nolint: object_usage_linter
##   lag_vec <- lag(vec, 1L)
##   if (inherits(vec, "numeric") && ! abs_tol %in% c(0, .Machine$double.xmin)) { # (no matrix/array/general support)
##     res <- if_else(
##       !is.na(vec) & !is.na(lag_vec),
##       near(vec, lag_vec, tol = abs_tol),
##       is.na(vec) & is.na(lag_vec)
##     )
##     return(res)
##   } else {
##     res <- vec_equal(vec, lag_vec, na_equal = TRUE)
##     return(res)
##   }
## }


map_ea <- function(.x, .f, ..., .is_locf = epiprocess:::is_locf, .clobberable_versions_start = NA, .compactify_tol = 0, .progress = FALSE) {

  if (length(.x) == 0L) {
    cli_abort("`.x` must have positive length")
  }

  .f <- as_mapper(.f)

  other_keys <- NULL
  previous_version <- NULL
  previous_snapshot <- NULL
  diffs <- map(.x, .progress = .progress, .f = function(.x_entry) {
    snapshot <- .f(.x_entry, ...)
    if (is_epi_df(snapshot)) {
      snapshot_other_keys <- attr(snapshot, "metadata")[["other_keys"]]
      version <- attr(snapshot, "metadata")[["as_of"]]
      snapshot <- as_tibble(snapshot) # drop metadata
      #
      # NOTE we might consider setDT here, but it didn't seem to make a
      # difference in a benchmark, and would require noting to user not to store
      # snapshot aliases from their computation
      snapshot <- as.data.table(snapshot)
    } else {
      cli_abort("`.f` produced a snapshot with an unsupported class:
                 {epiprocess:::format_class_vec(snapshot)}")
    }
    snapshot[, version := ..version]
    # snapshot has epikeytimeversion + value columns, but no key set.

    if (is.null(other_keys)) {
      other_keys <<- snapshot_other_keys
    } else if (!identical(other_keys, snapshot_other_keys)) {
      cli_abort("Inconsistent key settings; `other_keys` seemed to be
                 `c({epiprocess:::format_chr_with_quotes(other_keys)})` but then later
                 `c({epiprocess:::format_chr_with_quotes(snapshot_other_keys)})`.")
    }
    if (!is.null(previous_version) && previous_version >= version) {
      # XXX this could give a very delayed error on unsorted versions. Go back
      # to requiring .x to be a version list and validate that?
      cli_abort(c("Snapshots must be generated in ascending version order.",
                  "x" = "Version {previous_version} was followed by {version}.",
                  ">" = "If `.x` was a vector of version dates/tags, you might
                         just need to `sort` it."
                  ))
    }

    # Calculate diff as data.table with epikeytimeversion + value columns; we'll
    # rbindlist (and set key) afterward so it can have any or no key set.
    if (is.null(previous_snapshot)) {
      diff <- snapshot
    } else {
      # XXX consider profiling this setdiff approach as a special optimization for 0-tol case:
      #
      # FIXME doesn't consider deletions
      # diff <- setdiff(snapshot, previous_snapshot)
      #
      # Perform a miniature compactify operation between the two consecutive
      # versions:
      #
      # XXX consider refactoring `apply_compactify` into an S3 method and using
      # here (maybe only applicable if we get updates rather than snapshots from
      # the computations) or a setdiff.epi_df and work off of epi_dfs?
      #
      # TODO check deletions
      diff <- rbind(previous_snapshot, snapshot)
      setkeyv(diff, c("geo_value", other_keys, "time_value", "version"))
      diff <- diff[!Reduce(`&`, lapply(diff[, !"version"], .is_locf, abs_tol = .compactify_tol))]
      ekt_names <- c("geo_value", other_keys, "time_value")
      # XXX not general
      deletions <- previous_snapshot[
        !snapshot, ekt_names, on = ekt, with = FALSE
      ][
        , version := ..version
      ]
      diff <- rbind(diff, deletions, fill = TRUE)
      # `i` arg doesn't support ..var syntax; fake it:
      ..version <- version
      diff <- diff[version == ..version]
      # TODO swap in more efficient diffing approach _c from below?  except class generality and compactification tol ...
    }

    previous_version <<- version
    previous_snapshot <<- snapshot

    diff
  })

  diffs <- rbindlist(diffs)
  setkeyv(diffs, c("geo_value", other_keys, "time_value", "version"))
  setcolorder(diffs) # default: key first, then value cols

  as_epi_archive(
    diffs,
    other_keys = other_keys,
    clobberable_versions_start = .clobberable_versions_start,
    versions_end = previous_version,
    compactify = FALSE # we already compactified; don't re-do work or change tol
  )
}

map_ea(1:2, function(i) {
  as_epi_df(tibble(geo_value = 1, time_value = seq_len(i), value = seq_len(i)),
            as_of = i)
})

identical(epidatasets::archive_cases_dv_subset$DT %>% as_epi_archive(compactify = TRUE),
          map_ea(snapshots$slide_value, ~ .x,
                      .compactify_tol = .Machine$double.eps^0.5))

identical(epidatasets::archive_cases_dv_subset$DT %>% as_epi_archive(compactify = TRUE),
          map_ea(sort(unique(epidatasets::archive_cases_dv_subset$DT$version)),
                      ~ epidatasets::archive_cases_dv_subset %>% epix_as_of(.x),
                      .compactify_tol = .Machine$double.eps^0.5, .progress = TRUE))

bench::mark(
  lapply(seq_len(nrow(snapshots)), function(snapshot_i) {
    subres <- as_tibble(snapshots$slide_value[[snapshot_i]]) %>%
      mutate(version = snapshots$version[[snapshot_i]], .after = time_value)
    subres
  }) %>% bind_rows() %>% as_epi_archive(compactify = TRUE),
  map_ea(snapshots$slide_value, ~ .x,
              .compactify_tol = .Machine$double.eps^0.5),
  map_ea(snapshots$slide_value, ~ .x,
              is_locf = is_locf2,
              .compactify_tol = .Machine$double.eps^0.5),
  map_ea(snapshots$slide_value, ~ .x,
              is_locf = is_locf3,
              .compactify_tol = .Machine$double.eps^0.5),
  min_time = 30
)

# FIXME not saving time... though it may save our RAM.



profvis::profvis({
  map_ea(snapshots$slide_value, ~ .x,
              .compactify_tol = .Machine$double.eps^0.5)
})

system.time({
  ## value_nm <- "value"
  ## out_nm <- "value_7dsum"
  ## DT = copy(two_archive$DT)
  value_nm <- "case_rate_7d_av"
  out_nm <- "case_rate_7d_av_7dsum" # XXX nonsense, just testing
  ## DT = copy(epidatasets::archive_cases_dv_subset_all_states$DT)
  DT = copy(epidatasets::archive_cases_dv_subset$DT)
  epikeytime_colnames <- key(DT)[key(DT) != "version"]
  epikey_colnames <- epikeytime_colnames[epikeytime_colnames != "time_value"]
  # ascending versions
  nested <- DT[, list(data = list(.SD)), keyby = "version"]
  snapshot_dtbl <- DT[integer(0)][, version := NULL][] # (keyed by epikeytime_colnames)
  res <- map_ea(seq_len(nrow(nested)), function(version_i) {
    version <- nested$version[[version_i]]
    update_dtbl <- nested$data[[version_i]] # (keyed by epikeytime_colnames)
    snapshot_dtbl <<- unique(rbind(snapshot_dtbl, update_dtbl), by = epikeytime_colnames, fromLast = TRUE)
    setkeyv(snapshot_dtbl, epikeytime_colnames)
    # TODO try setDF for this conversion and then setDT after computations done?
    edf <- new_epi_df(as_tibble(as.data.frame(snapshot_dtbl)), geo_type = two_archive$geo_type, time_type = two_archive$time_type, as_of = version, other_keys = two_archive$other_keys)
    # TODO try windowing edf based on update range, either here or in
    # epi_slide_opt? More freedom in former since can potentially vary by geo.
    #
    # TODO try a DT-based approach? or dtplyr?
    edf %>%
      group_by(across(all_of(epikey_colnames))) %>%
      epi_slide_sum(.window_size = 7, all_of(value_nm)) %>%
      ungroup() %>%
      rename(!! out_nm := paste0("slide_value_", value_nm)) %>%
      {.}
  }, .progress = TRUE)
})




profvis::profvis({
  ## value_nm <- "value"
  ## out_nm <- "value_7dsum"
  ## DT = copy(two_archive$DT)
  value_nm <- "case_rate_7d_av"
  out_nm <- "case_rate_7d_av_7dsum" # XXX nonsense, just testing
  ## DT = copy(epidatasets::archive_cases_dv_subset_all_states$DT)
  DT = copy(epidatasets::archive_cases_dv_subset$DT)
  epikeytime_colnames <- key(DT)[key(DT) != "version"]
  epikey_colnames <- epikeytime_colnames[epikeytime_colnames != "time_value"]
  # ascending versions
  nested <- DT[, list(data = list(.SD)), keyby = "version"]
  snapshot_dtbl <- DT[integer(0)][, version := NULL][] # (keyed by epikeytime_colnames)
  res <- map_ea(seq_len(nrow(nested)), function(version_i) {
    version <- nested$version[[version_i]]
    update_dtbl <- nested$data[[version_i]] # (keyed by epikeytime_colnames)
    snapshot_dtbl <<- unique(rbind(snapshot_dtbl, update_dtbl), by = epikeytime_colnames, fromLast = TRUE)
    # XXX unnecessary?:
    setkeyv(snapshot_dtbl, epikeytime_colnames)
    # TODO try setDF for this conversion and then setDT after computations done?
    edf <- new_epi_df(as_tibble(as.data.frame(snapshot_dtbl)), geo_type = two_archive$geo_type, time_type = two_archive$time_type, as_of = version, other_keys = two_archive$other_keys)
    # TODO try windowing edf based on update range, either here or in
    # epi_slide_opt? More freedom in former since can potentially vary by geo.
    #
    # TODO try a DT-based approach? or dtplyr?
    edf %>%
      group_by(across(all_of(epikey_colnames))) %>%
      epi_slide_sum(.window_size = 7, all_of(value_nm)) %>%
      ungroup() %>%
      rename(!! out_nm := paste0("slide_value_", value_nm)) %>%
      {.}
  })
})




## epi_diff2 <- function(x, y) {
##   checkmate::assert_class(x, "epi_df")
##   checkmate::assert_class(y, "epi_df")

##   x_as_of <- attr(x, "metadata")$as_of
##   y_as_of <- attr(y, "metadata")$as_of

##   if (x_as_of >= y_as_of) {
##     cli::cli_abort(c("`y` should be a later snapshot than `x`",
##                      "*" = "`x` was as of {x_as_of}",
##                      "*" = "`y` was as of {y_as_of}"))
##   }


## }


apply_compactify0 <- function(updates_df, ukey_names, abs_tol = 0) {
  ## assert_data_frame(updates_df)
  ## assert_character(ukey_names)
  ## assert_subset(ukey_names, names(updates_df))
  ## if (vec_duplicate_any(ukey_names)) {
  ##   cli_abort("`ukey_names` must not contain duplicates")
  ## }
  ## if (length(ukey_names) == 0 || ukey_names[[length(ukey_names)]] != "version") {
  ##   cli_abort('"version" must appear in `ukey_names` and must be last.')
  ## }
  ## assert_numeric(abs_tol, len = 1, lower = 0)

  if (!is.data.table(updates_df) || !identical(key(updates_df), ukey_names)) {
    updates_df <- updates_df %>% arrange(pick(all_of(ukey_names)))
  }
  updates_df %>%
    filter(!update_is_locf(updates_df, ukey_names, abs_tol))
}

epi_diff2_a <- function(previous_snapshot, snapshot, .compactify_tol = .Machine$double.eps^0.5, rbinder = rbind, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- rbind(previous_snapshot, snapshot)
  diff_key_names <- c("geo_value", other_keys, "time_value", "version")
  setkeyv(diff, diff_key_names)
  ## diff <- diff[!Reduce(`&`, lapply(diff[, !"version"], .is_locf, abs_tol = .compactify_tol))]
  diff <- apply_compactify(diff, diff_key_names, abs_tol = .compactify_tol)
  # `i` arg doesn't support ..var syntax; fake it:
  ..version <- version
  diff <- diff[version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  # XXX not general
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- rbinder(diff, deletions, fill = TRUE)
  diff
}

epi_diff2_a0 <- function(previous_snapshot, snapshot, .compactify_tol = .Machine$double.eps^0.5, rbinder = rbind, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- rbind(previous_snapshot, snapshot)
  diff_key_names <- c("geo_value", other_keys, "time_value", "version")
  setkeyv(diff, diff_key_names)
  ## diff <- diff[!Reduce(`&`, lapply(diff[, !"version"], .is_locf, abs_tol = .compactify_tol))]
  diff <- apply_compactify0(diff, diff_key_names, abs_tol = .compactify_tol)
  # `i` arg doesn't support ..var syntax; fake it:
  ..version <- version
  diff <- diff[version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  # XXX not general
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- rbinder(diff, deletions, fill = TRUE)
  diff
}


# FIXME did move to apply_compactify / updated apply_compactify slow things down?

# FIXME default tol to 0
epi_diff2_a_re <- function(previous_snapshot, snapshot, .compactify_tol = .Machine$double.eps^0.5, rbinder = rbind, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- rbind(previous_snapshot, snapshot)
  edf_key_names <- c("geo_value", other_keys, "time_value")
  diff_key_names <- c("geo_value", other_keys, "time_value", "version")
  value_names <- setdiff(names(diff), diff_key_names)
  setkeyv(diff, diff_key_names)
  ## diff <- diff[!Reduce(`&`, lapply(diff[, !"version"], .is_locf, abs_tol = .compactify_tol))]
  ## diff <- apply_compactify(diff, diff_key_names, abs_tol = .compactify_tol)
  tmp <- as.list(diff)
  diff <- diff[!(
    Reduce(`&`, lapply(tmp[edf_key_names], .is_locf, .compactify_tol, TRUE)) &
      Reduce(`&`, lapply(tmp[value_names], .is_locf, .compactify_tol, FALSE))
  )]
  # `i` arg doesn't support ..var syntax; fake it:
  ..version <- version
  diff <- diff[version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  # XXX not general
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- rbinder(diff, deletions, fill = TRUE)
  diff
}


epi_diff2_a_nodeletion <- function(previous_snapshot, snapshot, .compactify_tol = .Machine$double.eps^0.5, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- rbind(previous_snapshot, snapshot)
  ## setkeyv(diff, c("geo_value", other_keys, "time_value", "version"))
  ## diff <- diff[!Reduce(`&`, lapply(diff[, !"version"], .is_locf, abs_tol = .compactify_tol))]
  diff_key_names <- c("geo_value", other_keys, "time_value", "version")
  setkeyv(diff, diff_key_names)
  diff <- apply_compactify0(diff, diff_key_names, abs_tol = .compactify_tol)
  # `i` arg doesn't support ..var syntax; fake it:
  ..version <- version
  diff <- diff[version == ..version]
  ## ekt_names <- c("geo_value", other_keys, "time_value")
  ## # XXX not general
  ## deletions <- previous_snapshot[
  ##   !snapshot, ekt_names, on = ekt_names, with = FALSE
  ## ][
  ## , version := ..version
  ## ]
  ## diff <- rbind(diff, deletions, fill = TRUE)
  diff
}

epi_diff2_b <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- rbind(previous_snapshot, snapshot)
  setkeyv(diff, c("geo_value", other_keys, "time_value", "version"))
  diff <- diff[!Reduce(`&`, lapply(diff[, !"version"], function(vec) {
    ## lag_vec <- lag(vec)
    ## if_else(
    ##   !is.na(vec) & !is.na(lag_vec),
    ##   vec == lag_vec,
    ##   is.na(vec) & is.na(lag_vec)
    ## )
    lag_vec <- lag(vec)
    res <- vec_equal(vec, lag_vec, na_equal = TRUE)
    res
  }))]
  # `i` arg doesn't support ..var syntax; fake it:
  ..version <- version
  diff <- diff[version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  # XXX not general
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- rbind(diff, deletions, fill = TRUE)
  diff
}

epi_diff2_c <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  # FIXME not general
  diff <- snapshot[!previous_snapshot, on = edf_names]
  ekt_names <- c("geo_value", other_keys, "time_value")
  # XXX not general
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- rbind(diff, deletions, fill = TRUE)
  diff
}

epi_diff2_c2 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  diff <- dtbl_anti_join_extract_b(snapshot, previous_snapshot, edf_names, all_names)
  ekt_names <- c("geo_value", other_keys, "time_value")
  # XXX not general
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- rbind(diff, deletions, fill = TRUE)
  diff
}

epi_diff2_c2c <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  diff <- dtbl_anti_join_extract_c(snapshot, previous_snapshot, edf_names, all_names)
  ekt_names <- c("geo_value", other_keys, "time_value")
  # XXX not general
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- rbind(diff, deletions, fill = TRUE)
  diff
}

epi_diff2_c2c2 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  diff <- dtbl_anti_join_extract_c(snapshot, previous_snapshot, edf_names, all_names)
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- dtbl_anti_join_extract_c(previous_snapshot, snapshot, ekt_names, ekt_names)
  diff <- rbind(diff, deletions, fill = TRUE)
  diff
}

epi_diff2_c2c2vc <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  diff <- dtbl_anti_join_extract_c(snapshot, previous_snapshot, edf_names, all_names)
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- dtbl_anti_join_extract_c(previous_snapshot, snapshot, ekt_names, ekt_names)
  diff <- vec_rbind(diff, deletions) # will add NA val col entries for deletions
  diff
}

epi_diff2_c2c2br <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  diff <- dtbl_anti_join_extract_c(snapshot, previous_snapshot, edf_names, all_names)
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- dtbl_anti_join_extract_c(previous_snapshot, snapshot, ekt_names, ekt_names)
  diff <- bind_rows(diff, deletions) # will add NA val col entries for deletions
  diff
}


epi_diff2_c3 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  diff <- dtbl_anti_join_extract(snapshot, previous_snapshot, edf_names, all_names)
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- dtbl_anti_join_extract(previous_snapshot, snapshot, ekt_names, ekt_names)[
  , version := ..version
  ]
  diff <- rbind(diff, deletions, fill = TRUE)
  diff
}

epi_diff2_c4 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  diff <- dtbl_anti_join_extract(snapshot, previous_snapshot, edf_names, all_names)
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- dtbl_anti_join_extract(previous_snapshot, snapshot, ekt_names, ekt_names)[
  , version := ..version
  ]
  diff <- vec_rbind(diff, deletions) # fills in NAs for deletions
  diff
}


epi_diff2_c5 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf, .compactify_tol = .Machine$double.eps^0.5) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  if (.compactify_tol == 0) {
    all_names <- names(snapshot)
    edf_names <- all_names[all_names != "version"]
    diff <- dtbl_anti_join_extract(snapshot, previous_snapshot, edf_names, all_names)
  } else {
    diff <- 
      FIXME
  }
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- dtbl_anti_join_extract(previous_snapshot, snapshot, ekt_names, ekt_names)[
  , version := ..version
  ]
  diff <- vec_rbind(diff, deletions) # fills
  diff
}


epi_diff2_c_delta_d_del <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  # FIXME not general
  diff <- snapshot[!previous_snapshot, on = edf_names]
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- anti_join(previous_snapshot, snapshot, by = ekt_names)[
  , ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- rbind(diff, deletions, fill = TRUE)
  diff
}

epi_diff2_d <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  diff <- anti_join(snapshot, previous_snapshot, by = edf_names)
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- anti_join(previous_snapshot, snapshot, by = ekt_names)[
  , ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- rbind(diff, deletions, fill = TRUE)
  diff
}

## dtbl_anti_join_extract0 <- function(dtbl1, dtbl2, by, out_colnames) {
##   # XXX pre-optimization: not checking types
##   #
##   # perf: if our join cols are simple, we can use data.table's join (faster);
##   # otherwise, use dplyr's (more general):
##   #
##   # perf: use as.list.data.table to avoid copy.
##   #
##   # TODO make test for this to warn about performance regressions?
##   if (!any(vapply(as.list(dtbl1)[by], is.list, FUN.VALUE = logical(1)))) {
##     dtbl1[!dtbl2, out_colnames, on = by, with = FALSE]
##   } else {
##     # XXX might not guarantee we don't have aliased columns?  though it seems to...
##     anti_join(dtbl1, dtbl2, by = by)[, out_colnames, with = FALSE]
##   }
## }
dtbl_anti_join_extract_a <- function(dtbl1, dtbl2, by, out_colnames) {
  # XXX pre-optimization: not checking types
  #
  # perf: if our join cols are simple, we can use data.table's join (faster);
  # otherwise, use dplyr's (more general):
  #
  # perf: use as.list.data.table to avoid copy.
  #
  # TODO make test for this to warn about performance regressions?
  if (!any(vapply(as.list(dtbl1)[by], is.list, FUN.VALUE = logical(1)))) {
    dtbl1[!dtbl2, out_colnames, on = by, with = FALSE]
  } else {
    subresult <- anti_join(dtbl1, dtbl2, by = by)
    # Note: anti_join could conceivably omit row-slicing dtbl1 and output an
    # alias of dtbl1 or perhaps of its columns if there's no overlap, but
    # doesn't seem to. Selecting `out_colnames` with `[.data.table` (with an
    # intermediate table alias lying around for good measure / reference count)
    # should ensure no unintended table/column aliasing.
    ## old_addrs <- sapply(subresult, address)
    # XXX could check for alias, conditionally copy, then try to find a mutating data.table op
    ## result <- subresult[, out_colnames, with = FALSE]
    result <- subresult[, .SD, .SDcols = out_colnames]
    ## new_addrs <- sapply(result, address)
    ## print(old_addrs == new_addrs)
    result
  }
}

#' Dealias & cure any ownership violations of a non-`{data.table}`-based rslice of a `data.table`
#'
#' In corner cases such as `orig_dtbl %>% filter(<all TRUE>)`,
#' non-`{data.table}`-based row-slicing operations may choose to simply return
#' `orig_dtbl` unchanged, aliasing the table, or to perform a shallow copy or
#' partially shallow copy, leaving all/some of the result&original columns
#' aliased. We generally don't have a guarantee in the documentation for these
#' functions that they won't do this.
#'
#' This function ensures that:
#' * we don't alias the original data table; and
#' * we don't violate `data.table`'s ownership model by having directly-aliased
#'   columns in the result or in the original.
#'
#' @param rslice_dtbl `data.table`; the result of performing row slicing
#' @param orig_dtbl `data.table`; the data table on which the row slicing was
#'   performed
#' @return `data.table`; "equivalent" of `rslice_dtbl` that has been dealiased
#'   or reconciled (via copying) if necessary.
#'
#' @examples
#'
#' original <- data.table(
#'   a = 1:10 + 0.2 # so not ALTREP (which couldn't be mutated anyway)
#' )
#'
#' alias <- original
#' alias_fixed <- dtbl_rslice_dealias_cure(alias, original)
#' address(alias) == address(original)
#' address(alias_fixed) == address(original)
#'
#' violator <- rlang::duplicate(original, shallow = TRUE)
#' violator_fixed <-  dtbl_rslice_dealias_cure(violator, original)
#' address(violator[[1]]) == address(original[[1]])
#' address(violator_fixed[[1]]) == address(original[[1]])
#'
#' @keywords internal
dtbl_rslice_dealias_cure <- function(rslice_dtbl, orig_dtbl) {
  if (nrow(rslice_dtbl) != nrow(orig_dtbl)) {
    # Typical case:
    return (rslice_dtbl)
  } else {
    # Potentially tricky case.  Perform the extra checks:
    if (
      address(rslice_dtbl) == address(orig_dtbl) ||
        any(vapply(rslice_dtbl, address, character(1)) ==
              vapply(orig_dtbl, address, character(1)))
    ) {
      # Do a full data table copy, even if it's just limited columns being
      # aliased.
      return (copy(rslice_dtbl))
    }
  }
}

# XXX vs. something with `pryr::refs`?
#
# XXX something with col[integer(0)] <- NULL?
#
# XXX could check for alias, conditionally copy, then try to find a mutating data.table op

dtbl_anti_join_extract_b <- function(dtbl1, dtbl2, by, out_colnames) {
  # XXX pre-optimization: not checking types
  #
  # perf: if our join cols are simple, we can use data.table's join (faster);
  # otherwise, use dplyr's (more general):
  #
  # perf: use as.list.data.table to avoid copy.
  #
  # TODO make test for this to warn about performance regressions?
  if (!any(vapply(as.list(dtbl1)[by], is.list, FUN.VALUE = logical(1)))) {
    dtbl1[!dtbl2, out_colnames, on = by, with = FALSE]
  } else {
    result <- anti_join(dtbl1, dtbl2, by = by)
    result <- dtbl_rslice_dealias_cure(result, dtbl1)
    ## old_addrs <- sapply(result, address)
    set(result, , setdiff(names(result), out_colnames), NULL)
    ## new_addrs <- sapply(result, address)
    ## print(old_addrs == new_addrs)
    result
  }
}

dtbl_anti_join_extract_c <- function(dtbl1, dtbl2, by, out_colnames) {
  # XXX pre-optimization: not checking types etc.
  #
  # perf: if our join cols are simple, we can use data.table's join (faster);
  # otherwise, use dplyr's (more general):
  #
  # perf: use as.list.data.table to avoid copy.
  #
  # TODO make test for this to warn about performance regressions?
  ## if (!any(vapply(as.list(dtbl1)[by], is.list, FUN.VALUE = logical(1)))) {
  ##   dtbl1[!dtbl2, out_colnames, on = by, with = FALSE]
  ## } else {
  ##   result <- anti_join(dtbl1, dtbl2, by = by)
  ##   result <- dtbl_rslice_dealias_cure(result, dtbl1)
  ##   ## old_addrs <- sapply(result, address)
  ##   set(result, , setdiff(names(result), out_colnames), NULL)
  ##   ## new_addrs <- sapply(result, address)
  ##   ## print(old_addrs == new_addrs)
  ##   result
  ## }
  by_col_refs_tbl1 <- as_tibble(as.list(dtbl1)[by])
  by_col_refs_tbl2 <- as_tibble(as.list(dtbl2)[by])
  ..overlapping <- vec_duplicate_detect(vec_rbind(by_col_refs_tbl1, by_col_refs_tbl2))
  length(..overlapping) <- nrow(by_col_refs_tbl1)
  dtbl1[!..overlapping, out_colnames, with = FALSE]
}

dtbl_anti_join_extract <- dtbl_anti_join_extract_b # FIXME make this selection _c

## dtbl_rbindlist <- function(dtbls) {
##   if (any(vapply(as.list(dtbls[[length(dtbls)]]), ......))) {
##     .........
##   } else {
##     .........
##   }
## }

## dtbl1x <- snapshots$slide_value[[400]] %>% as.data.table()
## dtbl1y <- snapshots$slide_value[[401]] %>% as.data.table()
## dtbl2x <- snapshots$slide_value[[400]] %>% mutate(fc = epipredict::dist_quantiles(as.list(case_rate_7d_av), 0.5)) %>% as.data.table()
## dtbl2y <- snapshots$slide_value[[401]] %>% mutate(fc = epipredict::dist_quantiles(as.list(case_rate_7d_av), 0.5)) %>% as.data.table()
## dtbl2px <- snapshots$slide_value[[400]] %>% mutate(fc = epipredict::dist_quantiles(as.list(case_rate_7d_av) %>% map(~ 0:10 * .x), 0:10/10)) %>% as.data.table()
## dtbl2py <- snapshots$slide_value[[401]] %>% mutate(fc = epipredict::dist_quantiles(as.list(case_rate_7d_av) %>% map(~ 0:10 * .x), 0:10/10)) %>% as.data.table()

## bench::mark(
##   dtbl1x[!dtbl1y, on = c("geo_value", "time_value")],
##   anti_join(dtbl1x, dtbl1y, by = c("geo_value", "time_value")),
##   dtbl_anti_join_extract_a(dtbl1x, dtbl1y, by = c("geo_value", "time_value"), names(dtbl1x)),
##   dtbl_anti_join_extract_b(dtbl1x, dtbl1y, by = c("geo_value", "time_value"), names(dtbl1x)),
##   dtbl_anti_join_extract_c(dtbl1x, dtbl1y, by = c("geo_value", "time_value"), names(dtbl1x)),
##   min_time = 1
## )

## bench::mark(
##   dtbl1y[!dtbl1x, on = c("geo_value", "time_value")],
##   anti_join(dtbl1y, dtbl1x, by = c("geo_value", "time_value")),
##   dtbl_anti_join_extract_a(dtbl1y, dtbl1x, by = c("geo_value", "time_value"), names(dtbl1y)),
##   dtbl_anti_join_extract_b(dtbl1y, dtbl1x, by = c("geo_value", "time_value"), names(dtbl1y)),
##   dtbl_anti_join_extract_c(dtbl1y, dtbl1x, by = c("geo_value", "time_value"), names(dtbl1y)),
##   min_time = 1
## )

## bench::mark(
##   ## dtbl2x[!dtbl2y, on = c("geo_value", "time_value", "fc")],
##   anti_join(dtbl2x, dtbl2y, by = c("geo_value", "time_value", "fc")),
##   dtbl_anti_join_extract_a(dtbl2x, dtbl2y, by = c("geo_value", "time_value", "fc"), names(dtbl2x)),
##   dtbl_anti_join_extract_b(dtbl2x, dtbl2y, by = c("geo_value", "time_value", "fc"), names(dtbl2x)),
##   dtbl_anti_join_extract_c(dtbl2x, dtbl2y, by = c("geo_value", "time_value", "fc"), names(dtbl2x)),
##   min_time = 1
## )


## bench::mark(
##   ## dtbl2x[!dtbl2y, on = c("geo_value", "time_value", "fc")],
##   anti_join(dtbl2x, dtbl2y, by = c("geo_value", "time_value", "fc")),
##   dtbl_anti_join_extract_a(dtbl2x, dtbl2y, by = c("geo_value", "time_value", "fc"), names(dtbl2x)),
##   dtbl_anti_join_extract_b(dtbl2x, dtbl2y, by = c("geo_value", "time_value", "fc"), names(dtbl2x)),
##   min_time = 1
## )

## bench::mark(
##   ## dtbl2y[!dtbl2x, on = c("geo_value", "time_value", "fc")],
##   anti_join(dtbl2y, dtbl2x, by = c("geo_value", "time_value", "fc")),
##   dtbl_anti_join_extract_a(dtbl2y, dtbl2x, by = c("geo_value", "time_value", "fc"), names(dtbl2x)),
##   dtbl_anti_join_extract_b(dtbl2y, dtbl2x, by = c("geo_value", "time_value", "fc"), names(dtbl2x)),
##   min_time = 1
## )

## bench::mark(
##   ## dtbl2px[!dtbl2py, on = c("geo_value", "time_value", "fc")],
##   anti_join(dtbl2px, dtbl2py, by = c("geo_value", "time_value", "fc")),
##   dtbl_anti_join_extract_a(dtbl2px, dtbl2py, by = c("geo_value", "time_value", "fc"), names(dtbl2px)),
##   dtbl_anti_join_extract_b(dtbl2px, dtbl2py, by = c("geo_value", "time_value", "fc"), names(dtbl2px)),
##   min_time = 1
## )

## bench::mark(
##   ## dtbl2py[!dtbl2px, on = c("geo_value", "time_value", "fc")],
##   anti_join(dtbl2py, dtbl2px, by = c("geo_value", "time_value", "fc")),
##   dtbl_anti_join_extract_a(dtbl2py, dtbl2px, by = c("geo_value", "time_value", "fc"), names(dtbl2px)),
##   dtbl_anti_join_extract_b(dtbl2py, dtbl2px, by = c("geo_value", "time_value", "fc"), names(dtbl2px)),
##   min_time = 1
## )


# XXX try to find some way to do "dispatch" selection only once rather than pay overhead every time? first check if this is important factor overall?
#
# --- issue seems mostly resolved already by avoiding copy

# FIXME don't use rbind and probably avoid / check before using rbindlist
## rbind(data.table(x = as.difftime(1, units = "days")),
##       data.table(x = as.difftime(1, units = "weeks")))

## vctrs::vec_c(data.table(x = as.difftime(1, units = "days")),
##              data.table(x = as.difftime(1, units = "weeks")))

## dplyr::bind_rows(data.table(x = as.difftime(1, units = "days")),
##                  data.table(x = as.difftime(1, units = "weeks")))

## bench::mark(
##   rbindlist(snapshots$slide_value %>% lapply(as_tibble) %>% lapply(as.data.table)),
##   bind_rows(snapshots$slide_value %>% lapply(as_tibble) %>% lapply(as.data.table)),
##   vctrs::vec_c(!!! snapshots$slide_value %>% lapply(as_tibble) %>% lapply(as.data.table)),
##   vctrs::vec_c(!!! snapshots$slide_value %>% lapply(as_tibble) %>% lapply(as.data.table) %>% lapply(as_tibble)) %>% as.data.table(),
##   vctrs::vec_c(!!! snapshots$slide_value %>% lapply(as_tibble) %>% lapply(as.data.table) %>% lapply(function(x) as_tibble(as.list(x)))) %>% as.data.table()
## )

epi_diff2_acd <- function(previous_snapshot, snapshot, .compactify_tol = .Machine$double.eps^0.5, rbinder = rbind, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  if (compactify_tol %in% c(0, .Machine$double.xmin)) {
    diff <- dtbl_anti_join_extract(snapshot, previous_snapshot, edf_names)
    ekt_names <- c("geo_value", other_keys, "time_value")
    # XXX not general
    deletions <- previous_snapshot[
      !snapshot, ekt_names, on = ekt_names, with = FALSE
    ][
    , version := ..version
    ]
    diff <- rbind(diff, deletions, fill = TRUE)
    diff
  } else {
    diff <- rbinder(previous_snapshot, snapshot)
    ## setkeyv(diff, c("geo_value", other_keys, "time_value", "version"))
    ## # TODO this is probably copying the table; avoid
    ## diff <- diff[!Reduce(`&`, lapply(diff[, !"version"], .is_locf, abs_tol = .compactify_tol))]
    diff_key_names <- c("geo_value", other_keys, "time_value", "version")
    setkeyv(diff, diff_key_names)
    diff <- apply_compactify0(diff, diff_key_names, abs_tol = .compactify_tol)
    # `i` arg doesn't support ..var syntax; fake it:
    ..version <- version
    diff <- diff[version == ..version]
  }
  ekt_names <- c("geo_value", other_keys, "time_value")
  # XXX not general
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- rbinder(diff, deletions, fill = TRUE)
  diff
}

# dplyr version of d, probably more general re. packed key cols, though if
# data.table ops still have spotty support for packed cols then we shouldn't be
# using them in key anyway:
epi_diff2_e <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as_tibble(previous_snapshot)
  previous_snapshot$version <- previous_version
  snapshot <- as_tibble(snapshot)
  snapshot$version <- version
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  diff <- anti_join(snapshot, previous_snapshot, by = edf_names)
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- anti_join(previous_snapshot, snapshot, by = ekt_names)[ekt_names]
  deletions$version <- version
  diff <- bind_rows(diff, deletions) # fills in NAs for value columns in deletions
  as.data.table(diff)
}

# TODO look into rbind vs. bind_rows vs. vec_c performance-wise

# dplyr version of a
epi_diff2_f <- function(previous_snapshot, snapshot, .compactify_tol = .Machine$double.eps^0.5, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as_tibble(previous_snapshot)
  previous_snapshot$version <- previous_version
  snapshot <- as_tibble(snapshot)
  snapshot$version <- version
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  diff <- rbind(previous_snapshot, snapshot)
  ## diff <- arrange(diff, pick(all_of(c("geo_value", other_keys, "time_value", "version"))))
  ## diff <- diff[!Reduce(`&`, lapply(diff[edf_names], .is_locf, abs_tol = .compactify_tol)), ]
  diff_key_names <- c("geo_value", other_keys, "time_value", "version")
  diff <- apply_compactify0(diff, diff_key_names, abs_tol = .compactify_tol)
  diff <- diff[diff$version == version, ]
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- anti_join(previous_snapshot, snapshot, by = ekt_names)
  deletions$version <- version
  diff <- rbind(diff, deletions) # fills in NAs for value columns in deletions
  as.data.table(diff)
}

# TODO full_join-based approach?

## full_join(tibble(df = snapshots$slide_value[[400]]), tibble(df = snapshots$slide_value[[401]]))

epi_diff2_g <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as_tibble(previous_snapshot)
  snapshot <- as_tibble(snapshot)
  edf_names <- names(snapshot)
  edf_key_names <- c("geo_value", other_keys, "time_value")
  val_names <- edf_names[! edf_names %in% edf_key_names]
  previous_snapshot <- tidyr::pack(previous_snapshot, .prev_vals = all_of(val_names))
  snapshot <- tidyr::pack(snapshot, .vals = all_of(val_names))
  diff <- full_join(previous_snapshot, snapshot, by = edf_key_names)
  diff <- diff[!vctrs::vec_equal(diff$.prev_vals, diff$.vals, na_equal = TRUE), ]
  diff <- diff[names(diff) != ".prev_vals"]
  diff <- unpack(diff, .vals)
  diff$version <- version
  as.data.table(diff)
}

epi_diff2_h <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- vec_rbind(previous_snapshot, snapshot)
  # Temporarily break data.table ownership model; don't leak this:
  diff_no_version <- as_tibble(as.list(diff)[setdiff(names(diff), "version")])
  ..version = version
  # XXX consider tibble conversion, vec_rbind, dup detect, tack on version,
  # duplicate&old&deletion removal instead of redundant&fancy version removal
  diff <- diff[!vec_duplicate_detect(diff_no_version) & version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- vec_rbind(diff, deletions)
  diff
}

epi_diff2_h2 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- vec_rbind(previous_snapshot, snapshot)
  # Temporarily break data.table ownership model; don't leak this:
  diff_no_version <- as_tibble(as.list(diff)[setdiff(names(diff), "version")])
  ..version <- version
  diff <- diff[!vec_duplicate_detect(diff_no_version) & version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- dtbl_anti_join_extract(previous_snapshot, snapshot, ekt_names, ekt_names)[
  , version := ..version
  ]
  diff <- vec_rbind(diff, deletions)
  diff
}

epi_diff2_h3 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- vec_rbind(previous_snapshot, snapshot)
  # Temporarily break data.table ownership model; don't leak this:
  diff_no_version <- as_tibble(as.list(diff)[setdiff(names(diff), "version")])
  ..locf <- unique(vec_duplicate_id(diff_no_version))
  diff <- diff[..locf]
  ..version <- version
  diff <- diff[version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- dtbl_anti_join_extract(previous_snapshot, snapshot, ekt_names, ekt_names)[
  , version := ..version
  ]
  diff <- vec_rbind(diff, deletions)
  diff
}

epi_diff2_i <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- rbind(previous_snapshot, snapshot)
  setkeyv(diff, c("geo_value", other_keys, "time_value", "version"))
  # Temporarily break data.table ownership model; don't leak this:
  diff_no_version <- as_tibble(as.list(diff)[setdiff(names(diff), "version")])
  ..version <- version
  run_sizes <- vec_run_sizes(diff_no_version)
  diff <- diff[vec_rep_each(run_sizes == 1, run_sizes) & version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- vec_rbind(diff, deletions)
  diff
}

epi_diff2_i2 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- rbind(previous_snapshot, snapshot)
  setkeyv(diff, c("geo_value", other_keys, "time_value", "version"))
  # Temporarily break data.table ownership model; don't leak this:
  diff_no_version <- as_tibble(as.list(diff)[setdiff(names(diff), "version")])
  ..version <- version
  run_sizes <- vec_run_sizes(diff_no_version)
  diff <- diff[
    vec_rep_each(rep(c(TRUE, FALSE), length(run_sizes)), vec_interleave(1, run_sizes - 1))
    & version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- vec_rbind(diff, deletions)
  diff
}

epi_diff2_i3 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- rbind(previous_snapshot, snapshot)
  setkeyv(diff, c("geo_value", other_keys, "time_value", "version"))
  # Temporarily break data.table ownership model; don't leak this:
  diff_no_version <- as_tibble(as.list(diff)[setdiff(names(diff), "version")])
  ..version <- version
  run_sizes <- vec_run_sizes(diff_no_version)
  ## diff <- diff[
  ##   vec_rep_each(rep(c(TRUE, FALSE), length(run_sizes)), vec_interleave(1, run_sizes - 1))
  ##   & version == ..version]
  diff <- diff[vec_rep_each(rep(c(TRUE, FALSE), length(run_sizes)), vec_interleave(1, run_sizes - 1))]
  diff <- diff[version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- vec_rbind(diff, deletions)
  diff
}

epi_diff2_i4 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- rbind(previous_snapshot, snapshot)
  setkeyv(diff, c("geo_value", other_keys, "time_value", "version"))
  # Temporarily break data.table ownership model; don't leak this:
  diff_no_version <- as_tibble(as.list(diff)[setdiff(names(diff), "version")])
  ..version <- version
  diff <- diff[!duplicated(diff_no_version) & version == ..version]
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- vec_rbind(diff, deletions)
  diff
}


epi_diff2_j <- function(previous_snapshot, snapshot, .is_locf = is_locf, .compactify_tol = 0) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  diff <- rbind(previous_snapshot, snapshot)
  ea_key_names <- c("geo_value", other_keys, "time_value", "version")
  setkeyv(diff, ea_key_names)
  ..version <- version
  diff <- apply_compactify0(diff, ea_key_names, abs_tol = .compactify_tol)[
    version == ..version
  ]
  ekt_names <- c("geo_value", other_keys, "time_value")
  deletions <- previous_snapshot[
    !snapshot, ekt_names, on = ekt_names, with = FALSE
  ][
  , version := ..version
  ]
  diff <- vec_rbind(diff, deletions)
  diff
}

epi_diff2_k <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  previous_version <- attr(previous_snapshot, "metadata")$as_of
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  previous_snapshot <- as.data.table(previous_snapshot)
  set(previous_snapshot, , "version", previous_version)
  snapshot <- as.data.table(snapshot)
  set(snapshot, , "version", version)
  all_names <- names(snapshot)
  edf_names <- all_names[all_names != "version"]
  ekt_names <- c("geo_value", other_keys, "time_value")
  val_names <- all_names[! all_names %in% c("geo_value", other_keys, "time_value", "version")]

  previous_edf_refs_tbl <- as_tibble(as.list(previous_snapshot)[edf_names])
  edf_refs_tbl <- as_tibble(as.list(snapshot)[edf_names])
  edfs_combined <- vec_rbind(previous_edf_refs_tbl, edf_refs_tbl)
  unchanged <- vec_duplicate_detect(edfs_combined)

  previous_ekt_refs_tbl <- as_tibble(as.list(previous_snapshot)[ekt_names])
  ekt_refs_tbl <- as_tibble(as.list(snapshot)[ekt_names])
  ekts_combined <- vec_rbind(previous_ekt_refs_tbl, ekt_refs_tbl)
  removed <- !vec_duplicate_detect(ekts_combined)
  length(removed) <- nrow(previous_ekt_refs_tbl)
  removed <- c(removed, rep(FALSE, nrow(ekt_refs_tbl)))

  include <- !unchanged & vec_rep_each(c(FALSE, TRUE), c(nrow(previous_ekt_refs_tbl), nrow(ekt_refs_tbl)))
  edfs_combined <- edfs_combined[include, ]
  edfs_combined[removed[include], val_names] <- NA

  edfs_combined$version <- version

  edfs_combined <- as.data.table(edfs_combined)

  edfs_combined
}

epi_diff2_k2 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  edf_names <- names(snapshot)
  ekt_names <- c("geo_value", other_keys, "time_value")
  val_names <- edf_names[! edf_names %in% ekt_names]

  previous_snapshot <- as_tibble(previous_snapshot)
  snapshot <- as_tibble(snapshot)

  snapshots_combined <- vec_rbind(previous_snapshot, snapshot)
  unchanged <- vec_duplicate_detect(snapshots_combined)

  previous_ekt_tbl <- previous_snapshot[ekt_names]
  ekt_tbl <- snapshot[ekt_names]
  ekts_combined <- vec_rbind(previous_ekt_tbl, ekt_tbl)
  removed <- !vec_duplicate_detect(ekts_combined)
  length(removed) <- nrow(previous_ekt_tbl)
  removed <- c(removed, rep(FALSE, nrow(ekt_tbl)))

  include <- !unchanged & vec_rep_each(c(FALSE, TRUE), c(nrow(previous_ekt_tbl), nrow(ekt_tbl)))
  snapshots_combined <- snapshots_combined[include, ]
  snapshots_combined[removed[include], val_names] <- NA

  snapshots_combined$version <- version

  snapshots_combined <- as.data.table(snapshots_combined)

  snapshots_combined
}

epi_diff2_k3 <- function(previous_snapshot, snapshot, .is_locf = epiprocess:::is_locf) {
  snapshot_metadata <- attr(snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  edf_names <- names(snapshot)
  ekt_names <- c("geo_value", other_keys, "time_value")
  val_names <- edf_names[! edf_names %in% ekt_names]

  previous_snapshot <- as_tibble(previous_snapshot)
  snapshot <- as_tibble(snapshot)

  snapshots_combined <- vec_rbind(previous_snapshot, snapshot)
  unchanged <- vec_duplicate_detect(snapshots_combined)

  ekts_combined <- snapshots_combined[ekt_names]
  removed <- !vec_duplicate_detect(ekts_combined)
  length(removed) <- nrow(previous_snapshot)
  removed <- c(removed, rep(FALSE, nrow(snapshot)))

  include <- !unchanged & vec_rep_each(c(FALSE, TRUE), c(nrow(previous_snapshot), nrow(snapshot)))
  snapshots_combined <- snapshots_combined[include, ]
  snapshots_combined[removed[include], val_names] <- NA

  snapshots_combined$version <- version

  snapshots_combined <- as.data.table(snapshots_combined)

  snapshots_combined
}

col_approx_equal <- function(vec1, vec2, abs_tol, is_key) {
  if (is_bare_numeric(vec1) && !is_key) {
    res <- if_else(
      !is.na(vec1) & !is.na(vec2),
      abs(vec1 - vec2) <= abs_tol,
      is.na(vec1) & is.na(vec2)
    )
    return(res)
  } else {
    res <- vec_equal(vec1, vec2, na_equal = TRUE)
    return(res)
  }
}

approx_equal <- function(vec1, vec2, abs_tol, na_equal, .ptype = NULL, recurse = approx_equal, inds1 = NULL, inds2 = NULL) {
  vecs <- list(vec1, vec2)
  if (!is.null(inds1)) {
    # could have logical or integerish inds; just leave it to later checks to error
  } else {
    vecs <- vec_recycle_common(!!!vecs)
  }
  vecs <- vec_cast_common(!!!vecs, .to = .ptype)
  approx_equal0(vecs[[1]], vecs[[2]], abs_tol, na_equal, rec = approx_equal, inds1, inds2)
}

approx_equal0 <- function(vec1, vec2, abs_tol, na_equal, recurse = approx_equal0, inds1 = NULL, inds2 = NULL) {
  if (is_bare_numeric(vec1)) {
    if (!is.null(inds1)) {
      vec1 <- vec1[inds1]
      vec2 <- vec2[inds2]
    }
    # perf: since we're working with bare numerics and logicals: we can use
    # fifelse, and stop it from propagating attributes when there's no special
    # class to guide the meaning
    res <- fifelse(
      !is.na(vec1) & !is.na(vec2),
      abs(vec1 - vec2) <= abs_tol,
      if (na_equal) is.na(vec1) & is.na(vec2) else FALSE
    )
    attributes(res) <- NULL
    return(res)
  } else if (is.data.frame(vec1)) {
    if (ncol(vec1) == 0) {
      rep(TRUE, nrow(vec1))
    } else {
      Reduce(`&`, lapply(seq_len(ncol(vec1)), function(col_i) {
        recurse(vec1[[col_i]], vec2[[col_i]], abs_tol, na_equal, recurse, inds1, inds2)
      }))
    }
  } else {
    # No special handling for any other types. Makes sense for unclassed atomic
    # things; bare lists and certain vctrs classes might want recursion /
    # specialization, though.
    if (!is.null(inds1)) {
      vec1 <- vec_slice(vec1, inds1)
      vec2 <- vec_slice(vec2, inds2)
    }
    res <- vec_equal(vec1, vec2, na_equal = na_equal, .ptype = .ptype)
    return(res)
  }
}

epi_diff2_l <- function(earlier_snapshot, later_snapshot,
                        .is_locf = epiprocess:::is_locf, .compactify_tol = .Machine$double.eps^0.5) {
  # Extract metadata:
  snapshot_metadata <- attr(later_snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  edf_names <- names(later_snapshot)
  ekt_names <- c("geo_value", other_keys, "time_value")
  val_names <- edf_names[! edf_names %in% ekt_names]
  earlier_n <- nrow(earlier_snapshot)
  later_n <- nrow(later_snapshot)

  # Convert to tibble and combine for duplicate detection (epi_df would
  # eventually complain):
  earlier_tbl <- as_tibble(earlier_snapshot)
  later_tbl <- as_tibble(later_snapshot)
  combined_tbl <- vec_rbind(earlier_tbl, later_tbl)
  combined_n <- nrow(combined_tbl)

  # We'll also need epikeytimes and value columns separately:
  combined_ekts <- combined_tbl[ekt_names]
  combined_vals <- combined_tbl[val_names]

  # We have five types of rows in combined_tbl:
  # 1. From earlier_tbl, no matching ekt in later_tbl (deletion; turn vals to
  #    NAs to match epi_archive format)
  # 2. From earlier_tbl, with matching ekt in later_tbl (context; exclude from
  #    result)
  # 3. From later_tbl, with matching ekt in earlier_tbl, with value "close" (change
  #    that we'll compactify away)
  # 4. From later_tbl, with matching ekt in earlier_tbl, value not "close" (change
  #    that we'll record)
  # 5. From later_tbl, with no matching ekt in later_tbl (addition)

  # We need to select out 1., 4., and 5., and alter values for 1.

  # Row indices of first occurrence of each ekt; will be the same as
  # seq_len(combined_n) except for when that ekt has been re-reported in
  # `later_snapshot`, in which case (3. or 4.) it will point back to the row index of
  # the same ekt in `earlier_snapshot`:
  combined_ekt_firsts <- vec_duplicate_id(combined_ekts)

  # Which rows from combined are cases 3. or 4.?
  combined_ekt_is_repeat <- combined_ekt_firsts != seq_len(combined_n)
  # For each row in 3. or 4., row numbers of the ekt appearance in earlier:
  ekt_repeat_first_i <- combined_ekt_firsts[combined_ekt_is_repeat]

  # Which rows from combined are in case 3.?
  combined_compactify_away <- rep(FALSE, combined_n)
  combined_compactify_away[combined_ekt_is_repeat] <-
    # Which rows from 3. & 4. that are in case 3.?
    #
    # TODO col_approx_equal and is_locf should support data.frame-class cols and
    # do this recursively:
    Reduce(
      `&`, lapply(seq_along(val_names), function(i) {
        val_col <- combined_vals[[i]]
        col_approx_equal(val_col[combined_ekt_is_repeat], val_col[ekt_repeat_first_i], .compactify_tol, FALSE)
      })
    )

  # Which rows from combined are in case 1.?
  combined_is_deletion <- vec_rep_each(c(TRUE, FALSE), c(earlier_n, later_n))
  combined_is_deletion[ekt_repeat_first_i] <- FALSE

  # Which rows from combined are in cases 3., 4., or 5.?
  combined_from_later <- vec_rep_each(c(FALSE, TRUE), c(earlier_n, later_n))

  # Which rows from combined are in cases 1., 4., or 5.?
  combined_include <- combined_is_deletion | combined_from_later & !combined_compactify_away
  combined_tbl <- combined_tbl[combined_include, ]
  # Represent deletion in 1. with NA-ing of all value columns. In some previous
  # approaches to epi_diff2, this seemed to be faster than using
  # vec_rbind(case_1_ekts, cases_45_tbl) or bind_rows to fill with NAs, and more
  # general than data.table's rbind(case_1_ekts, cases_45_tbl, fill = TRUE):
  combined_tbl[combined_is_deletion[combined_include], val_names] <- NA

  # XXX the version should probably be an attr at this point; this is for
  # compatibility with some other epi_diff2 variants being tested
  combined_tbl$version <- version

  combined_tbl <- as.data.table(combined_tbl)

  combined_tbl
}

epi_diff2_l2 <- function(earlier_snapshot, later_snapshot,
                        .is_locf = epiprocess:::is_locf, .compactify_tol = .Machine$double.eps^0.5) {
  # Extract metadata:
  snapshot_metadata <- attr(later_snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  edf_names <- names(later_snapshot)
  ekt_names <- c("geo_value", other_keys, "time_value")
  val_names <- edf_names[! edf_names %in% ekt_names]
  earlier_n <- nrow(earlier_snapshot)
  later_n <- nrow(later_snapshot)

  # Convert to tibble and combine for duplicate detection (epi_df would
  # eventually complain):
  earlier_tbl <- as_tibble(earlier_snapshot)
  later_tbl <- as_tibble(later_snapshot)
  combined_tbl <- vec_rbind(earlier_tbl, later_tbl)
  combined_n <- nrow(combined_tbl)

  # We'll also need epikeytimes and value columns separately:
  combined_ekts <- combined_tbl[ekt_names]
  combined_vals <- combined_tbl[val_names]

  # We have five types of rows in combined_tbl:
  # 1. From earlier_tbl, no matching ekt in later_tbl (deletion; turn vals to
  #    NAs to match epi_archive format)
  # 2. From earlier_tbl, with matching ekt in later_tbl (context; exclude from
  #    result)
  # 3. From later_tbl, with matching ekt in earlier_tbl, with value "close" (change
  #    that we'll compactify away)
  # 4. From later_tbl, with matching ekt in earlier_tbl, value not "close" (change
  #    that we'll record)
  # 5. From later_tbl, with no matching ekt in later_tbl (addition)

  # We need to select out 1., 4., and 5., and alter values for 1.

  # Row indices of first occurrence of each ekt; will be the same as
  # seq_len(combined_n) except for when that ekt has been re-reported in
  # `later_snapshot`, in which case (3. or 4.) it will point back to the row index of
  # the same ekt in `earlier_snapshot`:
  combined_ekt_firsts <- vec_duplicate_id(combined_ekts)

  # Which rows from combined are cases 3. or 4.?
  combined_ekt_is_repeat <- combined_ekt_firsts != seq_len(combined_n)
  # For each row in 3. or 4., row numbers of the ekt appearance in earlier:
  ekt_repeat_first_i <- combined_ekt_firsts[combined_ekt_is_repeat]

  # Which rows from combined are in case 3.?
  combined_compactify_away <- rep(FALSE, combined_n)
  combined_compactify_away[combined_ekt_is_repeat] <-
    # Which rows from 3. & 4. that are in case 3.?
    #
    # TODO col_approx_equal and is_locf should support data.frame-class cols and
    # do this recursively:
    Reduce(
      `&`, lapply(seq_along(val_names), function(i) {
        val_col <- combined_vals[[i]]
        col_approx_equal(val_col[combined_ekt_is_repeat], val_col[ekt_repeat_first_i], .compactify_tol, FALSE)
      })
    )

  # Which rows from combined are in case 1.?
  combined_is_deletion <- vec_rep_each(c(TRUE, FALSE), c(earlier_n, later_n))
  combined_is_deletion[ekt_repeat_first_i] <- FALSE

  # Which rows from combined are in cases 3., 4., or 5.?
  combined_from_later <- vec_rep_each(c(FALSE, TRUE), c(earlier_n, later_n))

  # Represent deletion in 1. with NA-ing of all value columns. In some previous
  # approaches to epi_diff2, this seemed to be faster than using
  # vec_rbind(case_1_ekts, cases_45_tbl) or bind_rows to fill with NAs, and more
  # general than data.table's rbind(case_1_ekts, cases_45_tbl, fill = TRUE):
  combined_tbl[combined_is_deletion, val_names] <- NA

  # Which rows from combined are in cases 1., 4., or 5.?
  combined_include <- combined_is_deletion | combined_from_later & !combined_compactify_away
  combined_tbl <- combined_tbl[combined_include, ]

  # XXX the version should probably be an attr at this point; this is for
  # compatibility with some other epi_diff2 variants being tested
  combined_tbl$version <- version

  combined_tbl <- as.data.table(combined_tbl)

  combined_tbl
}

epi_diff2_l3 <- function(earlier_snapshot, later_snapshot,
                         .is_locf = epiprocess:::is_locf, .compactify_tol = .Machine$double.eps^0.5) {
  # Extract metadata:
  snapshot_metadata <- attr(later_snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  edf_names <- names(later_snapshot)
  ekt_names <- c("geo_value", other_keys, "time_value")
  val_names <- edf_names[! edf_names %in% ekt_names]
  earlier_n <- nrow(earlier_snapshot)
  later_n <- nrow(later_snapshot)

  # Convert to tibble and combine for duplicate detection (epi_df would
  # eventually complain):
  earlier_tbl <- as_tibble(earlier_snapshot)
  later_tbl <- as_tibble(later_snapshot)
  combined_tbl <- vec_rbind(earlier_tbl, later_tbl)
  combined_n <- nrow(combined_tbl)

  # We'll also need epikeytimes and value columns separately:
  combined_ekts <- combined_tbl[ekt_names]
  combined_vals <- combined_tbl[val_names]

  # We have five types of rows in combined_tbl:
  # 1. From earlier_tbl, no matching ekt in later_tbl (deletion; turn vals to
  #    NAs to match epi_archive format)
  # 2. From earlier_tbl, with matching ekt in later_tbl (context; exclude from
  #    result)
  # 3. From later_tbl, with matching ekt in earlier_tbl, with value "close" (change
  #    that we'll compactify away)
  # 4. From later_tbl, with matching ekt in earlier_tbl, value not "close" (change
  #    that we'll record)
  # 5. From later_tbl, with no matching ekt in later_tbl (addition)

  # We need to select out 1., 4., and 5., and alter values for 1.

  # Row indices of first occurrence of each ekt; will be the same as
  # seq_len(combined_n) except for when that ekt has been re-reported in
  # `later_snapshot`, in which case (3. or 4.) it will point back to the row index of
  # the same ekt in `earlier_snapshot`:
  combined_ekt_firsts <- vec_duplicate_id(combined_ekts)

  # Which rows from combined are cases 3. or 4.?
  combined_ekt_is_repeat <- combined_ekt_firsts != seq_len(combined_n)
  # For each row in 3. or 4., row numbers of the ekt appearance in earlier:
  ekt_repeat_first_i <- combined_ekt_firsts[combined_ekt_is_repeat]

  # Which rows from combined are in case 3.?
  combined_compactify_away <- rep(FALSE, combined_n)
  combined_compactify_away[combined_ekt_is_repeat] <-
    approx_equal0(combined_vals,
                  combined_vals,
                  abs_tol = .compactify_tol,
                  na_equal = TRUE,
                  inds1 = combined_ekt_is_repeat,
                  inds2 = ekt_repeat_first_i
                  )

  # Which rows from combined are in case 1.?
  combined_is_deletion <- vec_rep_each(c(TRUE, FALSE), c(earlier_n, later_n))
  combined_is_deletion[ekt_repeat_first_i] <- FALSE

  # Which rows from combined are in cases 3., 4., or 5.?
  combined_from_later <- vec_rep_each(c(FALSE, TRUE), c(earlier_n, later_n))

  # Which rows from combined are in cases 1., 4., or 5.?
  combined_include <- combined_is_deletion | combined_from_later & !combined_compactify_away
  combined_tbl <- combined_tbl[combined_include, ]
  # Represent deletion in 1. with NA-ing of all value columns. In some previous
  # approaches to epi_diff2, this seemed to be faster than using
  # vec_rbind(case_1_ekts, cases_45_tbl) or bind_rows to fill with NAs, and more
  # general than data.table's rbind(case_1_ekts, cases_45_tbl, fill = TRUE):
  combined_tbl[combined_is_deletion[combined_include], val_names] <- NA

  # XXX the version should probably be an attr at this point; this is for
  # compatibility with some other epi_diff2 variants being tested
  combined_tbl$version <- version

  combined_tbl <- as.data.table(combined_tbl)

  combined_tbl
}

epi_diff2_l3tbl <- function(earlier_snapshot, later_snapshot,
                         .is_locf = epiprocess:::is_locf, .compactify_tol = .Machine$double.eps^0.5) {
  # Extract metadata:
  snapshot_metadata <- attr(later_snapshot, "metadata")
  other_keys <- snapshot_metadata$other_keys
  version <- snapshot_metadata$as_of
  edf_names <- names(later_snapshot)
  ekt_names <- c("geo_value", other_keys, "time_value")
  val_names <- edf_names[! edf_names %in% ekt_names]
  earlier_n <- nrow(earlier_snapshot)
  later_n <- nrow(later_snapshot)

  # Convert to tibble and combine for duplicate detection (epi_df would
  # eventually complain):
  earlier_tbl <- as_tibble(earlier_snapshot)
  later_tbl <- as_tibble(later_snapshot)
  combined_tbl <- vec_rbind(earlier_tbl, later_tbl)
  combined_n <- nrow(combined_tbl)

  # We'll also need epikeytimes and value columns separately:
  combined_ekts <- combined_tbl[ekt_names]
  combined_vals <- combined_tbl[val_names]

  # We have five types of rows in combined_tbl:
  # 1. From earlier_tbl, no matching ekt in later_tbl (deletion; turn vals to
  #    NAs to match epi_archive format)
  # 2. From earlier_tbl, with matching ekt in later_tbl (context; exclude from
  #    result)
  # 3. From later_tbl, with matching ekt in earlier_tbl, with value "close" (change
  #    that we'll compactify away)
  # 4. From later_tbl, with matching ekt in earlier_tbl, value not "close" (change
  #    that we'll record)
  # 5. From later_tbl, with no matching ekt in later_tbl (addition)

  # We need to select out 1., 4., and 5., and alter values for 1.

  # Row indices of first occurrence of each ekt; will be the same as
  # seq_len(combined_n) except for when that ekt has been re-reported in
  # `later_snapshot`, in which case (3. or 4.) it will point back to the row index of
  # the same ekt in `earlier_snapshot`:
  combined_ekt_firsts <- vec_duplicate_id(combined_ekts)

  # Which rows from combined are cases 3. or 4.?
  combined_ekt_is_repeat <- combined_ekt_firsts != seq_len(combined_n)
  # For each row in 3. or 4., row numbers of the ekt appearance in earlier:
  ekt_repeat_first_i <- combined_ekt_firsts[combined_ekt_is_repeat]

  # Which rows from combined are in case 3.?
  combined_compactify_away <- rep(FALSE, combined_n)
  combined_compactify_away[combined_ekt_is_repeat] <-
    approx_equal0(combined_vals,
                  combined_vals,
                  abs_tol = .compactify_tol,
                  na_equal = TRUE,
                  inds1 = combined_ekt_is_repeat,
                  inds2 = ekt_repeat_first_i
                  )

  # Which rows from combined are in case 1.?
  combined_is_deletion <- vec_rep_each(c(TRUE, FALSE), c(earlier_n, later_n))
  combined_is_deletion[ekt_repeat_first_i] <- FALSE

  # Which rows from combined are in cases 3., 4., or 5.?
  combined_from_later <- vec_rep_each(c(FALSE, TRUE), c(earlier_n, later_n))

  # Which rows from combined are in cases 1., 4., or 5.?
  combined_include <- combined_is_deletion | combined_from_later & !combined_compactify_away
  combined_tbl <- combined_tbl[combined_include, ]
  # Represent deletion in 1. with NA-ing of all value columns. In some previous
  # approaches to epi_diff2, this seemed to be faster than using
  # vec_rbind(case_1_ekts, cases_45_tbl) or bind_rows to fill with NAs, and more
  # general than data.table's rbind(case_1_ekts, cases_45_tbl, fill = TRUE):
  combined_tbl[combined_is_deletion[combined_include], val_names] <- NA

  # XXX the version should probably be an attr at this point; this is for
  # compatibility with some other epi_diff2 variants being tested
  combined_tbl$version <- version

  ## combined_tbl <- as.data.table(combined_tbl)

  combined_tbl
}


# TODO `complete` + `filter`-to-second-issue-based approaches?

# TODO unique-based approach?


# TODO disallow version as a column name in snapshots

bench::mark(
  epi_diff2_a(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_a_re(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_a_nodeletion(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_a_re(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_b(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_c(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_c2c(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_c2c2(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_d(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_e(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_f(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_g(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_h(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_h2(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_i(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_i2(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_j(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  epi_diff2_k(snapshots$slide_value[[400]], snapshots$slide_value[[401]]) |> setkeyv(c("geo_value","time_value","version")),
  min_time = 4
)

bench::mark(
  ## ## a = epi_diff2_a(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## a_re = epi_diff2_a_re(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## a_nodel = epi_diff2_a_nodeletion(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## b = epi_diff2_b(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## c = epi_diff2_c(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## c2 = epi_diff2_c2(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## c2c = epi_diff2_c2c(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## c2c2 = epi_diff2_c2c2(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## c3 = epi_diff2_c3(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## c4 = epi_diff2_c4(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## d = epi_diff2_d(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## e = epi_diff2_e(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## f = epi_diff2_f(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## g = epi_diff2_g(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## h = epi_diff2_h(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## h2 = epi_diff2_h2(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## h3 = epi_diff2_h3(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## i = epi_diff2_i(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## i2 = epi_diff2_i2(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## i3 = epi_diff2_i3(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## i4 = epi_diff2_i4(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## ## j = epi_diff2_j(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## k = epi_diff2_k(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## k2 = epi_diff2_k2(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  ## k3 = epi_diff2_k3(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  l = epi_diff2_l(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  l0 = epi_diff2_l(snapshots$slide_value[[400]], snapshots$slide_value[[401]], .compactify_tol = 0),
  l2 = epi_diff2_l2(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  l3 = epi_diff2_l3(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  check = FALSE, # ignore key/order differences.
  #
  # XXX consider testing also removing as.data.table conversions on some
  min_time = 4
) %>%
  print(n = 30)

bench::mark(
  epi_diff2_a(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_a_re(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_a_nodeletion(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_b(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_c(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_d(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_e(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_f(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_g(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_h(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_h2(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  epi_diff2_i(snapshots$slide_value[[200]], snapshots$slide_value[[201]]),
  check = FALSE, # ignore key/order differences.
  #
  # XXX consider testing also removing as.data.table conversions on some
  min_time = 4
)

bench::mark(
  epi_diff2_a(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_a_re(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_a_nodeletion(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_b(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_c(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_d(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_e(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_f(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_g(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_h(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_h2(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  epi_diff2_i(snapshots$slide_value[[500]], snapshots$slide_value[[501]]),
  check = FALSE, # ignore key/order differences.
  #
  # XXX consider testing also removing as.data.table conversions on some
  min_time = 4
)


bench::mark(
  epi_diff2_a(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_a_re(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_a_nodeletion(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_b(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_c(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_d(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_e(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_f(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_g(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_h(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_h2(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  epi_diff2_i(snapshots$slide_value[[10]], snapshots$slide_value[[11]]),
  check = FALSE, # ignore key/order differences.
  #
  # XXX consider testing also removing as.data.table conversions on some
  min_time = 4
)



map_snaps_ea <- function(.x, .f, ..., .is_locf = epiprocess:::is_locf, .clobberable_versions_start = NA, .compactify_tol = 0, .progress = FALSE) {
  if (length(.x) == 0L) {
    cli_abort("`.x` must have positive length")
  }

  .f <- as_mapper(.f)

  other_keys <- NULL
  previous_version <- NULL
  previous_snapshot <- NULL
  diffs <- map(.x, .progress = .progress, .f = function(.x_entry) {
    snapshot <- .f(.x_entry, ...)
    if (is_epi_df(snapshot)) {
      snapshot_other_keys <- attr(snapshot, "metadata")[["other_keys"]]
      version <- attr(snapshot, "metadata")[["as_of"]]
      snapshot <- as_tibble(snapshot) # drop metadata
      #
      # NOTE we might consider setDT here, but it didn't seem to make a
      # difference in a benchmark, and would require noting to user not to store
      # snapshot aliases from their computation
      snapshot <- as.data.table(snapshot)
    } else {
      cli_abort("`.f` produced a snapshot with an unsupported class:
                 {epiprocess:::format_class_vec(snapshot)}")
    }
    snapshot[, version := ..version]
    # snapshot has epikeytimeversion + value columns, but no key set.

    if (is.null(other_keys)) {
      other_keys <<- snapshot_other_keys
    } else if (!identical(other_keys, snapshot_other_keys)) {
      cli_abort("Inconsistent key settings; `other_keys` seemed to be
                 `c({epiprocess:::format_chr_with_quotes(other_keys)})` but then later
                 `c({epiprocess:::format_chr_with_quotes(snapshot_other_keys)})`.")
    }
    if (!is.null(previous_version) && previous_version >= version) {
      # XXX this could give a very delayed error on unsorted versions. Go back
      # to requiring .x to be a version list and validate that?
      cli_abort(c("Snapshots must be generated in ascending version order.",
                  "x" = "Version {previous_version} was followed by {version}.",
                  ">" = "If `.x` was a vector of version dates/tags, you might
                         just need to `sort` it."
                  ))
    }

    # Calculate diff as data.table with epikeytimeversion + value columns; we'll
    # rbindlist (and set key) afterward so it can have any or no key set.
    if (is.null(previous_snapshot)) {
      diff <- snapshot
    } else {
      # XXX consider profiling this setdiff approach as a special optimization for 0-tol case:
      #
      # FIXME doesn't consider deletions
      # diff <- setdiff(snapshot, previous_snapshot)
      #
      # Perform a miniature compactify operation between the two consecutive
      # versions:
      #
      # XXX consider refactoring `apply_compactify` into an S3 method and using
      # here (maybe only applicable if we get updates rather than snapshots from
      # the computations) or a setdiff.epi_df and work off of epi_dfs?
      #
      # TODO check deletions
      diff <- rbind(previous_snapshot, snapshot)
      setkeyv(diff, c("geo_value", other_keys, "time_value", "version"))
      ## diff <- diff[!Reduce(`&`, lapply(diff[, !"version"], .is_locf, abs_tol = .compactify_tol))]
      diff <- apply_compactify(diff, key(diff), .compactify_tol)
      ekt_names <- c("geo_value", other_keys, "time_value")
      # XXX not general
      deletions <- previous_snapshot[
        !snapshot, ekt_names, on = ekt_names, with = FALSE
      ][
        , version := ..version
      ]
      diff <- rbind(diff, deletions, fill = TRUE)
      # `i` arg doesn't support ..var syntax; fake it:
      ..version <- version
      diff <- diff[version == ..version]
      # TODO swap in more efficient diffing approach _c from below?  except class generality and compactification tol ...
    }

    previous_version <<- version
    previous_snapshot <<- snapshot

    diff
  })

  diffs <- rbindlist(diffs)
  setkeyv(diffs, c("geo_value", other_keys, "time_value", "version"))
  setcolorder(diffs) # default: key first, then value cols

  as_epi_archive(
    diffs,
    other_keys = other_keys,
    clobberable_versions_start = .clobberable_versions_start,
    versions_end = previous_version,
    compactify = FALSE # we already compactified; don't re-do work or change tol
  )
}
map_snaps_ea_mod <- function(.x, .f, ..., .epi_diff2 = epi_diff2_a, .is_locf = epiprocess:::is_locf, .rbindlist = rbindlist, .clobberable_versions_start = NA, .compactify_tol = 0, .progress = FALSE, .converter = as.data.table) {
  if (length(.x) == 0L) {
    cli_abort("`.x` must have positive length")
  }

  .f <- as_mapper(.f)

  other_keys <- NULL
  previous_version <- NULL
  previous_snapshot <- NULL
  diffs <- map(.x, .progress = .progress, .f = function(.x_entry) {
    snapshot <- .f(.x_entry, ...)
    if (is_epi_df(snapshot)) {
      snapshot_other_keys <- attr(snapshot, "metadata")[["other_keys"]]
      version <- attr(snapshot, "metadata")[["as_of"]]
    } else {
      cli_abort("`.f` produced a snapshot with an unsupported class:
                 {epiprocess:::format_class_vec(snapshot)}")
    }

    if (is.null(other_keys)) {
      other_keys <<- snapshot_other_keys
    } else if (!identical(other_keys, snapshot_other_keys)) {
      cli_abort("Inconsistent key settings; `other_keys` seemed to be
                 `c({epiprocess:::format_chr_with_quotes(other_keys)})` but then later
                 `c({epiprocess:::format_chr_with_quotes(snapshot_other_keys)})`.")
    }
    if (!is.null(previous_version) && previous_version >= version) {
      # XXX this could give a very delayed error on unsorted versions. Go back
      # to requiring .x to be a version list and validate that?
      cli_abort(c("Snapshots must be generated in ascending version order.",
                  "x" = "Version {previous_version} was followed by {version}.",
                  ">" = "If `.x` was a vector of version dates/tags, you might
                         just need to `sort` it."
                  ))
    }

    use_tol <- ".compactify_tol" %in% names(formals(.epi_diff2))

    # Calculate diff maybe as data.table with epikeytimeversion + value columns; we'll
    # rbindlist (and set key) afterward so it can have any or no key set.
    if (is.null(previous_snapshot)) {
      ## diff <- as.data.table(as_tibble(snapshot))
      diff <- as_tibble(snapshot)
      diff$version <- version
    } else {
      if (use_tol) {
        diff <- .epi_diff2(previous_snapshot, snapshot, .compactify_tol = .compactify_tol, .is_locf = .is_locf)
      } else {
        diff <- .epi_diff2(previous_snapshot, snapshot, .is_locf = .is_locf)
      }
    }

    previous_version <<- version
    previous_snapshot <<- snapshot

    diff
  })

  diffs <- .rbindlist(diffs)
  if (!is.data.table(diffs)) {
    ## diffs <- setDT(diffs) # FIXME check for aliasing or just as.data.table in only-1-with-nonzero-rows or all-zero-rows cases
    diffs <- .converter(diffs, key = c("geo_value", other_keys, "time_value", "version"))
  } else {
    setkeyv(diffs, c("geo_value", other_keys, "time_value", "version"))
  }
  setcolorder(diffs) # default: key first, then value cols

  as_epi_archive(
    diffs,
    other_keys = other_keys,
    clobberable_versions_start = .clobberable_versions_start,
    versions_end = previous_version,
    compactify = FALSE # we already compactified; don't re-do work or change tol
  )
}
map_updates_ea_mod <- function(...................) {
  ## TODO should be like map_snaps_ea_mod but with no deletion checking?
  ##
  ## TODO think of ways to share code
}

bench::mark(
  lgmem_no_del = lapply(seq_len(nrow(snapshots)), function(snapshot_i) {
    subres <- as_tibble(snapshots$slide_value[[snapshot_i]]) %>%
      mutate(version = snapshots$version[[snapshot_i]], .after = time_value)
    subres
  }) %>% bind_rows() %>% as_epi_archive(compactify = TRUE),
  nonmod = map_snaps_ea(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
  mod_a = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
  mod_a_br2 = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, rbinder = bind_rows),
  mod_a_vc2 = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, rbinder = vctrs::vec_c),
  mod_a_brlist = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, .rbindlist = bind_rows),
  mod_a_vclist = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, .rbindlist = function(dtbls) vec_c(!!!dtbls)),
  mod_a_vclistp = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, .rbindlist = function(dtbls) as.data.table(vec_c(!!!lapply(dtbls, as_tibble)))),
  mod_a_locf4 = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, .is_locf = is_locf4),
  mod_a_no_del = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_a_nodeletion, .compactify_tol = .Machine$double.eps^0.5),
  mod_b = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_b),
  mod_c = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c),
  mod_c_delta_d_del = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c_delta_d_del),
  mod_d = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_d),
  mod_e = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_e),
  mod_f = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_f, .compactify_tol = .Machine$double.eps^0.5),
  mod_g = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_g),
  mod_h = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_h),
  mod_h2 = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_h2),
  mod_i = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_i),
  mod_j = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_j),
  check = FALSE, # compactify tol vs. not, hopefully
  min_time = 5
)

bench::mark(
  mod_a = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
  mod_a_br2 = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, rbinder = bind_rows),
  mod_a_vc2 = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, rbinder = vctrs::vec_c),
  min_time = 5
)

bench::mark(
  mod_a = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
  mod_a_brlist = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, .rbindlist = bind_rows),
  mod_a_vclist = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, .rbindlist = function(dtbls) vec_c(!!!dtbls)),
  mod_a_vclistp = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, .rbindlist = function(dtbls) as.data.table(vec_c(!!!lapply(dtbls, as_tibble)))),
  min_time = 10
)

bench::mark(
  mod_a = map_snaps_ea_mod(fewer_snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
  mod_a_brlist = map_snaps_ea_mod(fewer_snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, .rbindlist = bind_rows),
  mod_a_vclist = map_snaps_ea_mod(fewer_snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, .rbindlist = function(dtbls) vec_c(!!!dtbls)),
  mod_a_vclistp = map_snaps_ea_mod(fewer_snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, .rbindlist = function(dtbls) as.data.table(vec_c(!!!lapply(dtbls, as_tibble)))),
  min_time = 10
)

bench::mark(
  mod_a = map_snaps_ea_mod(fewer_snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
  mod_a_br2 = map_snaps_ea_mod(fewer_snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, rbinder = bind_rows),
  mod_a_vc2 = map_snaps_ea_mod(fewer_snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5, rbinder = vctrs::vec_c),
  min_time = 10
)


bench::mark(
  lgmem_no_del = lapply(seq_len(nrow(snapshots)), function(snapshot_i) {
    subres <- as_tibble(snapshots$slide_value[[snapshot_i]]) %>%
      mutate(version = snapshots$version[[snapshot_i]], .after = time_value)
    subres
  }) %>% bind_rows() %>% as_epi_archive(compactify = TRUE),
  nonmod = map_snaps_ea(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
  mod_a = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
  mod_a_re = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_a_re, .compactify_tol = .Machine$double.eps^0.5),
  mod_a_no_del = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_a_nodeletion, .compactify_tol = .Machine$double.eps^0.5),
  mod_b = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_b),
  mod_c = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c),
  mod_c_delta_d_del = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c_delta_d_del),
  mod_d = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_d),
  mod_e = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_e),
  mod_f = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_f, .compactify_tol = .Machine$double.eps^0.5),
  mod_g = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_g),
  mod_h = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_h),
  mod_h2 = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_h2),
  mod_i = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_i),
  mod_j = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_j),
  check = FALSE, # compactify tol vs. not, hopefully
  min_time = 10
)


bench::mark(
  lgmem_no_del = lapply(seq_len(nrow(snapshots)), function(snapshot_i) {
    subres <- as_tibble(snapshots$slide_value[[snapshot_i]]) %>%
      mutate(version = snapshots$version[[snapshot_i]], .after = time_value)
    subres
  }) %>% bind_rows() %>% as_epi_archive(compactify = TRUE),
  mod_a = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
  mod_a0 = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_a_re, .compactify_tol = .Machine$double.eps^0.5),
  mod_a_re = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_a_re, .compactify_tol = .Machine$double.eps^0.5),
  mod_a_no_del = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_a_nodeletion, .compactify_tol = .Machine$double.eps^0.5),
  mod_h = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_h),
  mod_h2 = map_snaps_ea_mod(snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_h2),
  check = FALSE, # compactify tol vs. not, hopefully
  min_time = 10
)

# FIXME this doesn't include the actual smart logic.... this is more about mass
# snapshot file compactification...
setups <- tribble(
  ~snaps, ~DTthreads,
  snapshots, 6,
  alt_snapshots, 6,
  larger_fewer_snapshots, 6,
  snapshots, 1,
  alt_snapshots, 1,
  larger_fewer_snapshots, 1,
  )

withDTthreads <- withr::with_(
  get = function(new_n_threads) {
    getDTthreads()
  },
  set = function(new_n_threads) {
    setDTthreads(new_n_threads)
  }
)

for (setup_i in seq_len(nrow(setups))) {
  cli::cli_inform("Setup {setup_i}")
  test_snapshots <- setups[[setup_i, "snaps"]][[1]]
  n_threads <- setups[[setup_i, "DTthreads"]]
  # TODO print...
  withDTthreads(n_threads, {
    print(bench::mark(
      lgmem_no_del = lapply(seq_len(nrow(test_snapshots)), function(snapshot_i) {
        subres <- as_tibble(test_snapshots$slide_value[[snapshot_i]]) %>%
          mutate(version = test_snapshots$version[[snapshot_i]], .after = time_value)
        subres
      }) %>% bind_rows() %>% as_epi_archive(compactify = TRUE),
      ## nonmod = map_snaps_ea(test_snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
      ## a = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5),
      ## a0 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_a0, .compactify_tol = .Machine$double.eps^0.5),
      ## a_re = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_a_re, .compactify_tol = .Machine$double.eps^0.5),
      ## a_no_del = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_a_nodeletion, .compactify_tol = .Machine$double.eps^0.5),
      ## b = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_b),
      ## c = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c),
      ## c2 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c2),
      ## c2c = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c2c),
      ## c2c2 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c2c2),
      ## c2c2vc = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c2c2vc),
      ## c2c2br = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c2c2br),
      ## c3 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c3),
      ## c4 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c4),
      ## c_delta_d_del = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_c_delta_d_del),
      ## d = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_d),
      ## e = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_e),
      ## f = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_f, .compactify_tol = .Machine$double.eps^0.5),
      ## g = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_g),
      ## h = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_h),
      ## h2 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_h2),
      ## h3 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_h3),
      ## i = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_i),
      ## i2 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_i2),
      ## i3 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_i3),
      ## i4 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_i4),
      ## j = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_j),
      ## k = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_k),
      ## k2 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_k2),
      ## k3 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_k3),
      ## l = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l, .compactify_tol = .Machine$double.eps^0.5),
      ## l0 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l, .compactify_tol = 0),
      ## l2 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l2, .compactify_tol = .Machine$double.eps^0.5),
      ## l20 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l2, .compactify_tol = 0),
      ## l3 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l3, .compactify_tol = .Machine$double.eps^0.5),
      l30 = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l3, .compactify_tol = 0),
      ## l30_brl = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l3, .compactify_tol = 0, .rbindlist = bind_rows),
      ## l30_vrl = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l3, .compactify_tol = 0, .rbindlist = function(DTs) vec_rbind(!!!DTs)),
      ## l30_tblvrl_setDT = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l3tbl, .compactify_tol = 0, .rbindlist = function(DTs) vec_rbind(!!!DTs), .converter = setDT),
      l30_tblvrl_asDT = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l3tbl, .compactify_tol = 0, .rbindlist = function(DTs) vec_rbind(!!!DTs), .converter = as.data.table),
      l30_tblbrl_asDT = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l3tbl, .compactify_tol = 0, .rbindlist = bind_rows, .converter = as.data.table),
      l30_tblrbl = map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l3tbl, .compactify_tol = 0, .rbindlist = rbindlist, .converter = as.data.table),
      packed = map_ea(test_snapshots$slide_value, ~ .x, .compactify_tol = 0),
      ## check = FALSE, # compactify tol vs. not, hopefully
      min_time = 10
    ))
  })
}


## profvis::profvis({
##   map_snaps_ea_a(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5)
## })

Sys.setenv("PROFFER_PPROF_PATH" = "/usr/bin/pprof")

jointprof::joint_pprof({
  map_snaps_ea_mod(snapshots$slide_value, ~ .x, .compactify_tol = .Machine$double.eps^0.5)
})

jointprof::joint_pprof({
  withDTthreads(1, {
    map_snaps_ea_mod(snapshots$slide_value, ~ .x,
                     .epi_diff2 = epi_diff2_l3,
                     .compactify_tol = .Machine$double.eps^0.5)
  })
})

# TODO as_tibble.epi_df --- go through new_tibble not as_tibble? avoid overhead. Also think about whether vec_data is right.  Why not just use the decaying thing?

# TODO replace if_else with something more efficient? ifelse if it won't mess it up? data.table::fifelse? something using basic R?


simple_snap1 <-
  tibble(
    geo_value = 1, time_value = 1:3, pred = 1:3
  ) %>%
  as_epi_df(as_of = 1)

simple_snap2 <-
  tibble(
    geo_value = 1, time_value = 2:4, pred = c(2, 4, 5)
  ) %>%
  as_epi_df(as_of = 2)

epi_diff2_a(simple_snap1, simple_snap2)

forecast_snap1 <-
  tibble(
    geo_value = 1, time_value = 1:3, forecast = epipredict::dist_quantiles(list(1, 2, 3), 0.5)
  ) %>%
  as_epi_df(as_of = 1)

forecast_snap2 <-
  tibble(
    geo_value = 1, time_value = 2:4, forecast = epipredict::dist_quantiles(list(2, 4, 5), 0.5)
  ) %>%
  as_epi_df(as_of = 2)

## XXX might also want to test with packed key cols, but data.table has spotty
## support for that so could defer

epi_diff2_a(forecast_snap1, forecast_snap2)
epi_diff2_b(forecast_snap1, forecast_snap2)
## epi_diff2_c(forecast_snap1, forecast_snap2)
epi_diff2_d(forecast_snap1, forecast_snap2)
epi_diff2_e(forecast_snap1, forecast_snap2)
epi_diff2_f(forecast_snap1, forecast_snap2)
epi_diff2_g(forecast_snap1, forecast_snap2)

## TODO versions which diff based on integer version/.source (1 or 2, or maybe 0
## & max int?) rather than dtbl version with actual version column which might
## be harder to compare?



all.equal(
  lapply(seq_len(nrow(test_snapshots)), function(snapshot_i) {
    subres <- as_tibble(test_snapshots$slide_value[[snapshot_i]]) %>%
      mutate(version = test_snapshots$version[[snapshot_i]], .after = time_value)
    subres
  }) %>% bind_rows() %>% as_epi_archive(compactify_abs_tol = 5),
  map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l, .compactify_tol = 5)
)

all.equal(
  lapply(seq_len(nrow(test_snapshots)), function(snapshot_i) {
    subres <- as_tibble(test_snapshots$slide_value[[snapshot_i]]) %>%
      mutate(version = test_snapshots$version[[snapshot_i]], .after = time_value)
    subres
  }) %>% bind_rows() %>% as_epi_archive(compactify_abs_tol = 0),
  map_snaps_ea_mod(test_snapshots$slide_value, ~ .x, .epi_diff2 = epi_diff2_l, .compactify_tol = 0)
)


update_401 <- epi_diff2(snapshots$slide_value[[400]], snapshots$slide_value[[401]])


bench::mark(
  epi_diff2(snapshots$slide_value[[400]], snapshots$slide_value[[401]]),
  min_time = 6
)

bench::mark(
  epi_patch(snapshots$slide_value[[400]], update_401),
  min_time = 6
)

jointprof::joint_pprof({
  withDTthreads(1, {
    replicate(100, { epi_patch(snapshots$slide_value[[400]], update_401); 42})
  })
})
