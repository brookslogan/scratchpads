
# XXX vs tbl_diff_patch / tbl_diff2_patch?
tbl_diff2 <- function(earlier_tbl, later_tbl,
                      ukey_names,
                      input_format = c("snapshot", "update"),
                      compactify_abs_tol = 0) {

  # Most input validation. This is a small function so use faster validation
  # variants:
  if (!is_tibble(earlier_tbl)) {
    cli_abort("`earlier_tbl` must be a tibble")
  }
  if (!is_tibble(later_tbl)) {
    cli_abort("`later_tbl` must be a tibble")
  }
  input_format <- arg_match0(input_format, c("snapshot", "update"))
  if (!(is.vector(compactify_abs_tol, mode = "numeric") && length(compactify_abs_tol) == 1 && compactify_abs_tol >= 0)) {
    # Give a specific message:
    assert_numeric(compactify_abs_tol, lower = 0, any.missing = FALSE, len = 1)
    # Fallback e.g. for invalid classes not caught by assert_numeric:
    cli_abort("`compactify_abs_tol` must be a length-1 double/integer >= 0")
  }

  # Extract metadata:
  earlier_n <- nrow(earlier_tbl)
  later_n <- nrow(later_tbl)
  tbl_names <- names(earlier_tbl)
  val_names <- tbl_names[! tbl_names %in% ukey_names]

  # More input validation:
  if (!identical(tbl_names, names(later_tbl))) {
    # XXX is this check actually necessary?
    cli_abort(c("`earlier_tbl` and `later_tbl` should have identical column
                 names and ordering.",
                "*" = "`earlier_tbl` colnames: {format_chr_deparse(tbl_names)}",
                "*" = "`later_tbl` colnames: {format_chr_deparse(names(later_tbl))}"))
  }

  combined_tbl <- vec_rbind(earlier_tbl, later_tbl)
  combined_n <- nrow(combined_tbl)

  # We'll also need epikeytimes and value columns separately:
  combined_ukeys <- combined_tbl[ukey_names]
  combined_vals <- combined_tbl[val_names]

  # We have five types of rows in combined_tbl:
  # 1. From earlier_tbl, no matching ukey in later_tbl (deletion; turn vals to
  #    NAs to match epi_archive format)
  # 2. From earlier_tbl, with matching ukey in later_tbl (context; exclude from
  #    result)
  # 3. From later_tbl, with matching ukey in earlier_tbl, with value "close" (change
  #    that we'll compactify away)
  # 4. From later_tbl, with matching ukey in earlier_tbl, value not "close" (change
  #    that we'll record)
  # 5. From later_tbl, with no matching ukey in later_tbl (addition)

  # For "snapshot" input_format, we need to filter to 1., 4., and 5., and alter
  # values for 1.  For "update" input_format, we need to filter to 4. and 5.

  # (For compactify_abs_tol = 0, we could potentially streamline things by dropping
  # ukey+val duplicates (cases 2. and 3.).)

  # Row indices of first occurrence of each ukey; will be the same as
  # seq_len(combined_n) except for when that ukey has been re-reported in
  # `later_tbl`, in which case (3. or 4.) it will point back to the row index of
  # the same ukey in `earlier_tbl`:
  combined_ukey_firsts <- vec_duplicate_id(combined_ukeys)

  # Which rows from combined are cases 3. or 4.?
  combined_ukey_is_repeat <- combined_ukey_firsts != seq_len(combined_n)
  # For each row in 3. or 4., row numbers of the ukey appearance in earlier:
  ukey_repeat_first_i <- combined_ukey_firsts[combined_ukey_is_repeat]

  # Which rows from combined are in case 3.?
  combined_compactify_away <- rep(FALSE, combined_n)
  combined_compactify_away[combined_ukey_is_repeat] <-
    approx_equal0(combined_vals,
                  combined_vals,
                  # TODO move inds closer to vals to not be as confusing?
                  abs_tol = compactify_abs_tol,
                  na_equal = TRUE,
                  inds1 = combined_ukey_is_repeat,
                  inds2 = ukey_repeat_first_i)

  # Which rows from combined are in cases 3., 4., or 5.?
  combined_from_later <- vec_rep_each(c(FALSE, TRUE), c(earlier_n, later_n))

  if (input_format == "update") {
    # Cases 4. and 5.:
    combined_tbl <- combined_tbl[combined_from_later & !combined_compactify_away, ]
  } else { # input_format == "snapshot"
    # Which rows from combined are in case 1.?
    combined_is_deletion <- vec_rep_each(c(TRUE, FALSE), c(earlier_n, later_n))
    combined_is_deletion[ukey_repeat_first_i] <- FALSE
    # Which rows from combined are in cases 1., 4., or 5.?
    combined_include <- combined_is_deletion | combined_from_later & !combined_compactify_away
    combined_tbl <- combined_tbl[combined_include, ]
    # Represent deletion in 1. with NA-ing of all value columns. (In some
    # previous approaches to epi_diff2, this seemed to be faster than using
    # vec_rbind(case_1_ukeys, cases_45_tbl) or bind_rows to fill with NAs, and more
    # general than data.table's rbind(case_1_ukeys, cases_45_tbl, fill = TRUE).)
    combined_tbl[combined_is_deletion[combined_include], val_names] <- NA
  }

  combined_tbl
}

# XXX vs. tbl_patch_apply?
tbl_patch <- function(snapshot, update, ukey_names) {
  # Most input validation. This is a small function so use faster validation
  # variants:
  if (!is_tibble(snapshot)) {
    cli_abort("`snapshot` must be a tibble")
  }
  if (!is_tibble(update)) {
    # XXX debating about whether to have a specialized class for updates/diffs.
    # Seems nice for type-based reasoning and might remove some args from
    # interfaces, but would require constructor/converter functions for that
    # type.
    cli_abort("`update` must be a tibble")
  }

  result_tbl <- vec_rbind(update, snapshot)

  dup_ids <- vec_duplicate_id(result_tbl[ukey_names])
  not_overwritten <- dup_ids == vec_seq_along(result_tbl)
  result_tbl <- result_tbl[not_overwritten,]

  ## result_tbl <- arrange_canonical(result_tbl)

  result_tbl
}

# for one group, minus group keys
epix_epi_slide_sub <- function(updates, before, after, time_type) {
  unit_step <- epiprocess:::unit_time_delta(time_type)
  prev_inp_snapshot <- NULL
  prev_out_snapshot <- NULL
  map(seq_len(nrow(updates)), function(update_i) {
    version <- updates$version[[update_i]]
    ## if (version == as.Date("2020-08-02")) browser()
    ## browser()
    inp_update <- updates$subtbl[[update_i]] # TODO decide whether DT
    setDF(inp_update)
    inp_update <- as_tibble(inp_update)
    inp_update$.real <- TRUE
    if (is.null(prev_inp_snapshot)) {
      inp_snapshot <- inp_update
    } else {
      inp_snapshot <- tbl_patch(prev_inp_snapshot, inp_update, "time_value")
    }
    inp_update_min_t <- min(inp_update$time_value) # TODO check efficiency
    inp_update_max_t <- max(inp_update$time_value)
    ## out_update_min_t <- inp_update_min_t - after * unit_step
    ## out_update_max_t <- inp_update_max_t + before * unit_step
    ## slide_min_t <- out_update_min_t - before * unit_step
    ## slide_max_t <- out_update_max_t + after * unit_step
    slide_min_t <- inp_update_min_t - (before + after) * unit_step
    slide_max_t <- inp_update_max_t + (before + after) * unit_step
    slide_n <- time_delta_to_n_steps(slide_max_t - slide_min_t, time_type) + 1L
    slide_time_values <- slide_min_t + 0:(slide_n - 1) * unit_step
    inds <- vec_match(slide_time_values, inp_snapshot$time_value)
    slide <- inp_snapshot[inds, ] # TODO vs. DT key index vs ....
    slide$time_value <- slide_time_values
    # TODO ensure before & after as integers?
    # TODO parameterize naming, slide function, options, ...
    slide$slide_value <- frollmean(slide$value, before + after + 1)
    slide <- slide[seq(1L + before, nrow(slide) - after), ]
    ## slide <- slide[slide$.real, names(slide) != ".real"]
    slide <- slide[!is.na(slide$.real), names(slide) != ".real"]
    if (is.null(prev_out_snapshot)) {
      # TODO move these NULL checks etc. into the diff2 and patch functions?
      # and/or find a better value than NULL?
      slide_update <- slide
      out_snapshot <- slide
    } else {
      slide_update <- tbl_diff2(prev_out_snapshot, slide, "time_value", "update") # TODO parms
      out_snapshot <- tbl_patch(prev_out_snapshot, slide_update)
    }
    slide_update$version <- version
    prev_inp_snapshot <<- inp_snapshot
    prev_out_snapshot <<- out_snapshot # TODO avoid need to patch twice?
    slide_update
  })
}


grp_updates <- test_archive$DT[, list(data = list(.SD)), keyby = geo_value]$data[[1L]][,list(time_value, version, value = percent_cli, case_rate_7d_av)][, list(subtbl = list(.SD)), keyby = version]

test_subresult <-
  epix_epi_slide_sub(grp_updates, 6, 0, "day") %>%
  rbindlist() %>%
  ## `[`((.real), !".real") %>%
  setkeyv(c("time_value", "version")) %>%
  setcolorder() %>%
  `[`()

expected <- mean_archive2$DT[geo_value == "ca", !"geo_value"] %>% rename(value = percent_cli, slide_value = percent_cli_7dav) %>% `[`()

test_subresult %>%
  count(time_value)
expected %>%
  count(time_value)

waldo::compare(
  test_subresult %>%
    count(time_value, is.na(slide_value)) %>%
    print(topn = 20),
  expected %>%
    count(time_value, is.na(slide_value)) %>%
    print(topn = 20)
)

waldo::compare(
  test_subresult[time_value == as.Date("2020-06-01")],
  expected[time_value == as.Date("2020-06-01")] %>% setkeyv(c("time_value", "version")) %>% `[`()
)

waldo::compare(
  test_subresult[time_value == as.Date("2020-06-03")],
  expected[time_value == as.Date("2020-06-03")] %>% setkeyv(c("time_value", "version")) %>% `[`()
)

waldo::compare(
  test_subresult[time_value == as.Date("2020-06-06")],
  expected[time_value == as.Date("2020-06-06")] %>% setkeyv(c("time_value", "version")) %>% `[`()
)

test_subresult[time_value == as.Date("2020-06-06")][
  !vec_equal(test_subresult[time_value == as.Date("2020-06-06")]$slide_value,
             expected[time_value == as.Date("2020-06-06")] %>% setkeyv(c("time_value", "version")) %>% `[`() %>% .$slide_value, na_equal = TRUE)
]

mean_archive1 %>%
  epix_as_of(as.Date("2020-06-10"))



waldo::compare(
  test_subresult,
  expected %>% setkeyv(c("time_value", "version")) %>% `[`()
)
# FIXME DEBUG

withDTthreads(1, {
  bench::mark(epix_epi_slide_sub(grp_updates, 6, 0, "day"))
})

.trace_time_ns <- rlang::new_environment()
.trace_time_ts <- rlang::new_environment()
.trace_time_dts <- rlang::new_environment()
# overwrites any existing tracer and deletes tracers added midway
with_eager_and_trace_time <- function(what, code, where = topenv(parent.frame())) {
  what_sym <- ensym(what)
  ## where <- environment()
  ## where <- quo_get_env(enquo(what))
  ## where <- topenv(parent.frame())
  ## where <- environment(what)
  ## print(where)
  what_str <- as.character(what_sym)
  what_str_expr <- what_str
  .GlobalEnv[[".trace_time_ns"]][[what_str]] <- 0L
  .GlobalEnv[[".trace_time_dts"]][[what_str]] <- as.difftime(0, units = "secs")
  # TODO better keys that just what_str
  on.exit({
    untrace(what_sym, where = where)
    cli_inform('{what_str} was called { .GlobalEnv[[".trace_time_ns"]][[what_str]]}
                time{?s} and used {format(.GlobalEnv[[".trace_time_dts"]][[what_str]])}')
  })
  entry <- call2("{", !!!c(
    lapply(syms(formalArgs(args(what))), function(arg_sym) {
      expr(force(!!arg_sym))
    }),
    list(expr({
      .GlobalEnv[[".trace_time_ns"]][[!!what_str_expr]] <- .GlobalEnv[[".trace_time_ns"]][[!!what_str_expr]] + 1L
      .GlobalEnv[[".trace_time_ts"]][[!!what_str_expr]] <- Sys.time()
    }))
  ))
  exit <- expr({
    .GlobalEnv[[".trace_time_dts"]][[!!what_str_expr]] <- .GlobalEnv[[".trace_time_dts"]][[!!what_str_expr]] + (Sys.time() - .GlobalEnv[[".trace_time_ts"]][[!!what_str_expr]])
  })
  trace(what = what_sym,
        tracer = entry,
        exit = exit,
        where = where,
        print = FALSE)
  code
}

with_eager_and_trace_time(frollmean, frollmean(1:100, 7))

with_eager_and_trace_time(
  frollmean, where = asNamespace("data.table"),
  covid_case_death_rates_extended %>%
    epi_slide_mean(case_rate, .window_size = 7) %>%
    with(median(case_rate_7dav))
)

with_eager_and_trace_time(epix_as_of
                          ## , where = asNamespace("epiprocess")
                          ## , where = environment(epix_as_of)
                        , {
     library(dplyr)
     # Reference time points for which we want to compute slide values:
     versions <- seq(as.Date("2020-06-02"),
       as.Date("2020-06-15"),
       by = "1 day"
     )
     # A simple (but not very useful) example (see the archive vignette for a more
     # realistic one):
     archive_cases_dv_subset %>%
       group_by(geo_value) %>%
       epix_slide(
         .f = ~ mean(.x$case_rate_7d_av),
         .before = 2,
         .versions = versions,
         .new_col_name = "case_rate_7d_av_recent_av"
       ) %>%
       ungroup()
     # TODO remove this bad example from docs
})

frollmean_dt <- as.difftime(0, units = "secs")
trace(frollmean,
      quote({force(x); .GlobalEnv$frollmean_t <- Sys.time()}),
      quote(.GlobalEnv$frollmean_dt <- .GlobalEnv$frollmean_dt + (Sys.time() - .GlobalEnv$frollmean_t)),
      print = FALSE)
system.time(epix_epi_slide_sub(grp_updates, 6, 0, "day"))
frollmean_dt
untrace(frollmean)

jointprof::joint_pprof({
  withDTthreads(1, {
    epix_epi_slide_sub(grp_updates, 6, 0, "day")
  })
})

profvis::profvis({
  withDTthreads(1, {
    epix_epi_slide_sub(grp_updates, 6, 0, "day")
  })
})

system.time({
  with_eager_and_trace_time(frollmean, # where = asNamespace("data.table"),
                            invisible(epix_epi_slide_sub(grp_updates, 6, 0, "day")))
})

system.time({
  with_eager_and_trace_time(min,
                            ## invisible(epix_epi_slide_sub(grp_updates, 6, 0, "day"))
                            print(min)
                            )
})


# XXX consider getting a zero_time_value and converting time_values to integers? might require ensuring time_value ordering in some places...
