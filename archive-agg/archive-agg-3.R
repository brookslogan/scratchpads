



stop("migrated to epiprocess branch, with bugfixes")





# XXX naming below vs tbl_diff_patch / tbl_diff2_patch?




#' Calculate compact patch to move from one snapshot/update to another
#'
#' @param earlier_snapshot tibble or `NULL`; `NULL` represents that there was no
#'   data before `later_tbl`.
#' @param later_tbl tibble; must have the same column names as
#'   `earlier_snapshot` if it is a tibble.
#' @param ukey_names character; column names that together, form a unique key
#'   for `earlier_snapshot` and for `later_tbl`. This is unchecked; see
#'   [`check_ukey_unique`] if you don't already have this guaranteed.
#' @param later_format "snapshot" or "update"; default is "snapshot". If
#'   "snapshot", `later_tbl` will be interpreted as a full snapshot of the data
#'   set including all ukeys, and any ukeys that are in `earlier_snapshot` but
#'   not in `later_tbl` are interpreted as deletions, which are currently
#'   (imprecisely) represented in the output patch as revisions of all
#'   non-`ukey_names` columns to NA values (using `{vctrs}`). If "update", then
#'   it's assumed that any deletions have already been represented this way in
#'   `later_tbl` and any ukeys not in `later_tbl` are simply unchanged; we are
#'   just ensuring that the update is fully compact for the given
#'   `compactify_abs_tol`.
#' @param compactify_abs_tol compactification tolerance; see `apply_compactify`
#' @return a tibble in compact "update" (diff) format
tbl_diff2 <- function(earlier_snapshot, later_tbl,
                      ukey_names,
                      later_format = c("snapshot", "update"),
                      compactify_abs_tol = 0) {
  # Most input validation + handle NULL earlier_snapshot. This is a small function so
  # use faster validation variants:
  if (!is_tibble(later_tbl)) {
    cli_abort("`later_tbl` must be a tibble")
  }
  if (is.null(earlier_snapshot)) {
    return(later_tbl)
  }
  if (!is_tibble(earlier_snapshot)) {
    cli_abort("`earlier_snapshot` must be a tibble or `NULL`")
  }
  later_format <- arg_match0(later_format, c("snapshot", "update"))
  if (!(is.vector(compactify_abs_tol, mode = "numeric") && length(compactify_abs_tol) == 1L && compactify_abs_tol >= 0)) {
    # Give a specific message:
    assert_numeric(compactify_abs_tol, lower = 0, any.missing = FALSE, len = 1L)
    # Fallback e.g. for invalid classes not caught by assert_numeric:
    cli_abort("`compactify_abs_tol` must be a length-1 double/integer >= 0")
  }

  # Extract metadata:
  earlier_n <- nrow(earlier_snapshot)
  later_n <- nrow(later_tbl)
  tbl_names <- names(earlier_snapshot)
  val_names <- tbl_names[! tbl_names %in% ukey_names]

  # More input validation:
  if (!identical(tbl_names, names(later_tbl))) {
    # XXX is this check actually necessary?
    cli_abort(c("`earlier_snapshot` and `later_tbl` should have identical column
                 names and ordering.",
                "*" = "`earlier_snapshot` colnames: {format_chr_deparse(tbl_names)}",
                "*" = "`later_tbl` colnames: {format_chr_deparse(names(later_tbl))}"))
  }

  combined_tbl <- vec_rbind(earlier_snapshot, later_tbl)
  combined_n <- nrow(combined_tbl)

  # We'll also need epikeytimes and value columns separately:
  combined_ukeys <- combined_tbl[ukey_names]
  combined_vals <- combined_tbl[val_names]

  # We have five types of rows in combined_tbl:
  # 1. From earlier_snapshot, no matching ukey in later_tbl (deletion; turn vals to
  #    NAs to match epi_archive format)
  # 2. From earlier_snapshot, with matching ukey in later_tbl (context; exclude from
  #    result)
  # 3. From later_tbl, with matching ukey in earlier_snapshot, with value "close" (change
  #    that we'll compactify away)
  # 4. From later_tbl, with matching ukey in earlier_snapshot, value not "close" (change
  #    that we'll record)
  # 5. From later_tbl, with no matching ukey in later_tbl (addition)

  # For "snapshot" later_format, we need to filter to 1., 4., and 5., and alter
  # values for 1.  For "update" later_format, we need to filter to 4. and 5.

  # (For compactify_abs_tol = 0, we could potentially streamline things by dropping
  # ukey+val duplicates (cases 2. and 3.).)

  # Row indices of first occurrence of each ukey; will be the same as
  # seq_len(combined_n) except for when that ukey has been re-reported in
  # `later_tbl`, in which case (3. or 4.) it will point back to the row index of
  # the same ukey in `earlier_snapshot`:
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

  if (later_format == "update") {
    # Cases 4. and 5.:
    combined_tbl <- combined_tbl[combined_from_later & !combined_compactify_away, ]
  } else { # later_format == "snapshot"
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



#' Apply an update (e.g., from `tbl_diff2`) to a snapshot
#'
#' @param snapshot tibble or `NULL`; entire data set as of some version, or
#'   `NULL` to treat `update` as the initial version of the data set.
#' @param update tibble; ukeys + initial values for added rows, ukeys + new
#'   values for changed rows. Deletions must be imprecisely represented as
#'   changing all values to NAs.
#' @param ukey_names character; names of columns that should form a unique key
#'   for `snapshot` and for `update`. Uniqueness is unchecked; if you don't have
#'   this guaranteed, see [`check_ukey_unique()`].
#' @return tibble; snapshot of the data set with the update applied.
tbl_patch <- function(snapshot, update, ukey_names) {
  # Most input validation. This is a small function so use faster validation
  # variants:
  if (!is_tibble(update)) {
    # XXX debating about whether to have a specialized class for updates/diffs.
    # Seems nice for type-based reasoning and might remove some args from
    # interfaces, but would require constructor/converter functions for that
    # type.
    cli_abort("`update` must be a tibble")
  }
  if (is.null(snapshot)) {
    return(update)
  }
  if (!is_tibble(snapshot)) {
    cli_abort("`snapshot` must be a tibble")
  }

  result_tbl <- vec_rbind(update, snapshot)

  dup_ids <- vec_duplicate_id(result_tbl[ukey_names])
  not_overwritten <- dup_ids == vec_seq_along(result_tbl)
  result_tbl <- result_tbl[not_overwritten,]

  ## result_tbl <- arrange_canonical(result_tbl)

  result_tbl
}

# for one group, minus group keys
epix_epi_slide_sub <- function(updates, in_colnames, f, before, after, time_type, out_colnames) {
  unit_step <- epiprocess:::unit_time_delta(time_type)
  prev_inp_snapshot <- NULL
  prev_out_snapshot <- NULL
  result <- map(seq_len(nrow(updates)), function(update_i) {
    version <- updates$version[[update_i]]
    inp_update <- updates$subtbl[[update_i]] # TODO decide whether DT
    ## setDF(inp_update)
    ## inp_update <- new_tibble(inp_update, nrow = nrow(inp_update))
    inp_snapshot <- tbl_patch(prev_inp_snapshot, inp_update, "time_value")
    inp_update_min_t <- min(inp_update$time_value) # TODO check efficiency
    inp_update_max_t <- max(inp_update$time_value)
    # If the input had updates in the range t1..t2, this could produce changes
    # in slide outputs in the range t1-after..t2+before, and to compute those
    # slide values, we need to look at the input snapshot from
    # t1-after-before..t2+before+after.
    slide_min_t <- inp_update_min_t - (before + after) * unit_step
    slide_max_t <- inp_update_max_t + (before + after) * unit_step
    slide_n <- time_delta_to_n_steps(slide_max_t - slide_min_t, time_type) + 1L
    slide_time_values <- slide_min_t + 0L:(slide_n - 1L) * unit_step
    slide_inp_backrefs <- vec_match(slide_time_values, inp_snapshot$time_value)
    slide <- inp_snapshot[slide_inp_backrefs, ] # TODO vs. DT key index vs ....
    slide$time_value <- slide_time_values
    # TODO ensure before & after as integers?
    # TODO parameterize naming, slide function, options, ...
    for (col_i in seq_along(in_colnames)) {
      slide[[out_colnames[[col_i]]]] <- f(slide[[in_colnames[[col_i]]]], before + after + 1L)
    }
    out_update <- slide[
      # Get back to t1-after..t2+before; times outside this range were included
      # only so those inside would have enough context for their slide
      # computations, but these "context" rows may contain invalid slide
      # computation outputs:
      vec_rep_each(c(FALSE, TRUE, FALSE), c(before, slide_n - before - after, after)) &
        # Only include time_values that appeared in the input snapshot:
        !is.na(slide_inp_backrefs),
    ]
    out_diff <- tbl_diff2(prev_out_snapshot, slide, "time_value", "update") # TODO parms
    out_snapshot <- tbl_patch(prev_out_snapshot, out_diff)
    prev_inp_snapshot <<- inp_snapshot
    prev_out_snapshot <<- out_snapshot # TODO avoid need to patch twice?
    out_diff$version <- version
    out_diff
  })
  result
}


epix_epi_slide_opt3 <-
  function(.x, .col_names, .f, ...,
           .window_size = NULL, .align = c("right", "center", "left"),
           .prefix = NULL, .suffix = NULL, .new_col_names = NULL#,
           ## .ref_time_values = NULL, .all_rows = FALSE
           ) {
  UseMethod("epix_epi_slide_opt3")
}

epix_epi_slide_opt3.grouped_epi_archive <- function(.x, ...) {
  assert_set_equal(group_vars(.x),
                   key_colnames(.x, exclude = c("time_value", "version")))
  orig_group_vars <- group_vars(.x)
  orig_drop <- .x$private$drop
  .x %>%
    ungroup() %>%
    epix_epi_slide_opt3(...) %>%
    group_by(pick(all_of(orig_group_vars)), .drop = orig_drop)
}

upstream_slide_f_info <- function(.f) {
  # Check that slide function `.f` is one of those short-listed from
  # `data.table` and `slider` (or a function that has the exact same definition,
  # e.g. if the function has been reexported or defined locally). Extract some
  # metadata. `namer` will be mapped over columns (.x will be a column, not the
  # entire edf).
  f_possibilities <-
    tibble::tribble(
      ~f, ~package, ~namer,
      frollmean, "data.table", ~ if (is.logical(.x)) "prop" else "av",
      frollsum, "data.table", ~ if (is.logical(.x)) "count" else "sum",
      frollapply, "data.table", ~"slide",
      slide_sum, "slider", ~ if (is.logical(.x)) "count" else "sum",
      slide_prod, "slider", ~"prod",
      slide_mean, "slider", ~ if (is.logical(.x)) "prop" else "av",
      slide_min, "slider", ~"min",
      slide_max, "slider", ~"max",
      slide_all, "slider", ~"all",
      slide_any, "slider", ~"any",
    )
  f_info_row <- f_possibilities %>%
    filter(map_lgl(.data$f, ~ identical(.f, .x)))
  if (nrow(f_info_row) == 0L) {
    # `f` is from somewhere else and not supported
    cli_abort(
      c(
        "problem with {rlang::expr_label(rlang::caller_arg(f))}",
        "i" = "`f` must be one of `data.table`'s rolling functions (`frollmean`,
              `frollsum`, `frollapply`. See `?data.table::roll`) or one of
              `slider`'s specialized sliding functions (`slide_mean`, `slide_sum`,
              etc. See `?slider::\`summary-slide\`` for more options)."
      ),
      class = "epiprocess__epi_slide_opt__unsupported_slide_function",
      epiprocess__f = .f
    )
  }
  if (nrow(f_info_row) > 1L) {
    cli_abort('epiprocess internal error: looking up `.f` in table of possible
               functions yielded multiple matches. Please report it using "New
               issue" at https://github.com/cmu-delphi/epiprocess/issues, using
               reprex::reprex to provide a minimal reproducible example.')
  }
  f_from_package <- f_info_row$package
  list(
    from_package = f_from_package,
    namer = unwrap(f_info_row$namer)
  )
}

across_ish_names_info <- function(.x, time_type, col_names_quo, .f_namer, .window_size, .align, .prefix, .suffix, .new_col_names) {
  # The position of a given column can be differ between input `.x` and
  # `.data_group` since the grouping step by default drops grouping columns.
  # To avoid rerunning `eval_select` for every `.data_group`, convert
  # positions of user-provided `col_names` into string column names. We avoid
  # using `names(pos)` directly for robustness and in case we later want to
  # allow users to rename fields via tidyselection.
  pos <- eval_select(col_names_quo, data = .x, allow_rename = FALSE)
  col_names_chr <- names(.x)[pos]

  # Handle output naming
  if ((!is.null(.prefix) || !is.null(.suffix)) && !is.null(.new_col_names)) {
    cli_abort(
      "Can't use both .prefix/.suffix and .new_col_names at the same time.",
      class = "epiprocess__epi_slide_opt_incompatible_naming_args"
    )
  }
  assert_string(.prefix, null.ok = TRUE)
  assert_string(.suffix, null.ok = TRUE)
  assert_character(.new_col_names, len = length(col_names_chr), null.ok = TRUE)
  if (is.null(.prefix) && is.null(.suffix) && is.null(.new_col_names)) {
    .suffix <- "_{.n}{.time_unit_abbr}{.align_abbr}{.f_abbr}"
    # ^ does not account for any arguments specified to underlying functions via
    # `...` such as `na.rm =`, nor does it distinguish between functions from
    # different packages accomplishing the same type of computation. Those are
    # probably only set one way per task, so this probably produces cleaner
    # names without clashes (though maybe some confusion if switching between
    # code with different settings).
  }
  if (!is.null(.prefix) || !is.null(.suffix)) {
    .prefix <- .prefix %||% ""
    .suffix <- .suffix %||% ""
    if (identical(.window_size, Inf)) {
      n <- "running_"
      time_unit_abbr <- ""
      align_abbr <- ""
    } else {
      n <- time_delta_to_n_steps(.window_size, time_type)
      time_unit_abbr <- time_type_unit_abbr(time_type)
      align_abbr <- c(right = "", center = "c", left = "l")[[.align]]
    }
    glue_env <- rlang::env(
      .n = n,
      .time_unit_abbr = time_unit_abbr,
      .align_abbr = align_abbr,
      .f_abbr = purrr::map_chr(.x[, c(col_names_chr)], .f_namer), # compat between DT and tbl selection
      quo_get_env(col_names_quo)
    )
    .new_col_names <- unclass(
      glue(.prefix, .envir = glue_env) +
        col_names_chr +
        glue(.suffix, .envir = glue_env)
    )
  } else {
    # `.new_col_names` was provided by user; we don't need to do anything.
  }
  if (any(.new_col_names %in% names(.x))) {
    cli_abort(c(
      "Naming conflict between new columns and existing columns",
      "x" = "Overlapping names: {format_varnames(intersect(.new_col_names, names(.x)))}"
    ), class = "epiprocess__epi_slide_opt_old_new_name_conflict")
  }
  if (anyDuplicated(.new_col_names)) {
    cli_abort(c(
      "New column names contain duplicates",
      "x" = "Duplicated names: {format_varnames(unique(.new_col_names[duplicated(.new_col_names)]))}"
    ), class = "epiprocess__epi_slide_opt_new_name_duplicated")
  }
  result_col_names <- .new_col_names

  return(list(
    col_names_chr = col_names_chr,
    result_col_names = result_col_names
  ))
}

epix_epi_slide_opt3.epi_archive <-
  function(.x, .col_names, .f, ...,
           .window_size = NULL, .align = c("right", "center", "left"),
           .prefix = NULL, .suffix = NULL, .new_col_names = NULL
           ## , .ref_time_values = NULL, .all_rows = FALSE
           ) {
    # Extract metadata:
    time_type <- .x$time_type
    epikey_names <- key_colnames(.x, exclude = c("time_value", "version"))
    # Validation & pre-processing:
    .align <- arg_match(.align)
    f_info <- upstream_slide_f_info(.f)
    col_names_quo <- enquo(.col_names)
    names_info <- across_ish_names_info(.x$DT, time_type, col_names_quo, f_info$namer, .window_size, .align, .prefix, .suffix, .new_col_names)
    window_args <- get_before_after_from_window(.window_size, .align, time_type)
    # Perform the slide:
    updates_grouped <- .x$DT %>%
      as.data.frame() %>%
      as_tibble(.name_repair = "minimal") %>%
      # 0 rows input -> 0 rows output, so we can just say drop = TRUE:
      grouped_df(epikey_names, TRUE)
    ## pb_id <- cli_progress_bar("Epigroups", n_groups(updates_grouped)) # TODO
    updates_grouped %>%
      group_modify(function(group_values, group_key) {
        # FIXME TODO from_package & handling within epix_epi_slide_sub
        group_updates <- group_values %>% nest(.by = version, .key = "subtbl")
        res <- epix_epi_slide_sub(group_updates, names_info$col_names_chr, .f, window_args$before, window_args$after, time_type, names_info$result_col_names) %>%
          list_rbind()
        ## cli_progress_update(id = pb_id)
        res
      })
    ## cli_progress_done(id = pb_id)
    # FIXME TODO format back to archive
  }


grp_updates <- test_archive$DT[, list(data = list(.SD)), keyby = geo_value]$data[[1L]][, list(subtbl = list(.SD)), keyby = version]
updates_by_group <- test_archive$DT[, list(data = list(.SD)), keyby = geo_value]$data %>% lapply(function(x) x[, list(subtbl = list(.SD)), keyby = version])


test_subresult <-
  epix_epi_slide_sub(grp_updates, "percent_cli", frollmean, 6, 0, "day", "percent_cli_7dav") %>%
  rbindlist() %>%
  ## `[`((.real), !".real") %>%
  setkeyv(c("time_value", "version")) %>%
  setcolorder() %>%
  `[`()

expected <- mean_archive1$DT[geo_value == "ca", !"geo_value"]

test_subresult %>%
  count(time_value)
expected %>%
  count(time_value)

waldo::compare(
  test_subresult %>%
    count(time_value, is.na(percent_cli_7dav)) %>%
    as.data.frame() %>%
    as_tibble(),
  expected %>%
    count(time_value, is.na(percent_cli_7dav)) %>%
    as.data.frame() %>%
    as_tibble()
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
  !vec_equal(test_subresult[time_value == as.Date("2020-06-06")]$percent_cli_7dav,
             expected[time_value == as.Date("2020-06-06")] %>% setkeyv(c("time_value", "version")) %>% `[`() %>% .$percent_cli_7dav, na_equal = TRUE)
]

mean_archive1 %>%
  epix_as_of(as.Date("2020-06-10"))



waldo::compare(
  test_subresult,
  expected %>% setkeyv(c("time_value", "version")) %>% `[`()
)

withDTthreads(1, {
  ## bench::mark(epix_epi_slide_sub(grp_updates, "percent_cli", frollmean, 6, 0, "day", "percent_cli_7dav"),
  ##             min_time = 3)
  bench::mark(
    lapply(updates_by_group, function(grp_updates) epix_epi_slide_sub(grp_updates, "percent_cli", frollmean, 6, 0, "day", "percent_cli_7dav"))
  )
})


bench::mark(
  test_archive$DT[, list(data = list(.SD)), keyby = geo_value]$data %>% lapply(function(x) x[, list(subtbl = list(.SD)), keyby = version]),
  test_archive$DT %>% group_by(geo_value) %>% group_map(function(grp_data, grp_key) {
    grp_data %>% nest(.by = version)
  }),
  check = FALSE
)

withDTthreads(1, {
  jointprof::joint_pprof({
    updates_by_group <- test_archive$DT[, list(data = list(.SD)), keyby = geo_value]$data %>% lapply(function(x) x[, list(subtbl = list(.SD)), keyby = version])
    lapply(updates_by_group, function(grp_updates) epix_epi_slide_sub(grp_updates, "percent_cli", frollmean, 6, 0, "day", "percent_cli_7dav")) %>%
      list_flatten() %>%
      list_rbind()
  })
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
  t0 <- Sys.time()
  on.exit({
    t1 <- Sys.time()
    prop <- as.numeric(.GlobalEnv[[".trace_time_dts"]][[what_str]], units = "secs") / as.numeric(t1 - t0, units = "secs")
    untrace(what_sym, where = where)
    cli_inform('{what_str} was called { .GlobalEnv[[".trace_time_ns"]][[what_str]]}
                time{?s} and used {format(.GlobalEnv[[".trace_time_dts"]][[what_str]])};
                {100*prop}% of elapsed.')
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
  invisible(code)
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
    print(system.time({
      epix_epi_slide_sub(grp_updates, 6, 0, "day")
    }))
  })
})

profvis::profvis({
  withDTthreads(1, {
    print(system.time({
      epix_epi_slide_sub(grp_updates, 6, 0, "day")
    }))
  })
})

system.time({
  with_eager_and_trace_time(frollmean, # where = asNamespace("data.table"),
                            invisible(epix_epi_slide_sub(grp_updates, 6, 0, "day")))
})

system.time({
  with_eager_and_trace_time(min,
                            invisible(epix_epi_slide_sub(grp_updates, 6, 0, "day"))
                            )
})

invisible(epix_epi_slide_sub(grp_updates, "percent_cli", frollmean, 6, 0, "day", "percent_cli_7dav"))


# XXX consider getting a zero_time_value and converting time_values to integers? might require ensuring time_value ordering in some places...

# TODO data.table version?
d401_402 <- epi_diff2(snapshots$slide_value[[401]], snapshots$slide_value[[402]])
d401_402tbl <- as_tibble(d401_402)

edf401 <- snapshots$slide_value[[401]]
edf402 <- snapshots$slide_value[[402]]
tbl401 <- as_tibble(edf401)
tbl402 <- as_tibble(edf402)

bench::mark(
  epi_diff2(edf401, edf402),
  tbl_diff2(tbl401, tbl402, c("geo_value", "time_value")),
  check = FALSE
)

jointprof::joint_pprof(
  withDTthreads(1, {
    for (i in 1:10000) epi_diff2(edf401, edf402)
  })
)

jointprof::joint_pprof(
  withDTthreads(1, {
    for (i in 1:10000) tbl_diff2(tbl401, tbl402, c("geo_value", "time_value"))
  })
)

bench::mark(
  epi_patch(edf401, d401_402),
  tbl_patch(tbl401, d401_402tbl, c("geo_value", "time_value")),
  check = FALSE
)


jointprof::joint_pprof(
  withDTthreads(1, {
    for (i in 1:10000) epi_patch(edf401, d401_402)
  })
)

jointprof::joint_pprof(
  withDTthreads(1, {
    for (i in 1:10000) tbl_patch(tbl401, d401_402tbl, c("geo_value", "time_value"))
  })
)

# XXX seems like there's a lot of time spent in as_tibble.epi_df; can we avoid?
# Is this reflected in slide profiling results?

epi_patch(snapshots$slide_value[[401]], d401_402)

DT401 <- as.data.table(as_tibble(snapshots$slide_value[[401]]), key = c("geo_value", "time_value"))
DT401_402 <- d401_402 %>% as_tibble() %>% as.data.table(key = c("geo_value", "time_value"))

# TODO finish

# TODO try vec_unique_loc? not sure if it guarantees it will return the first appearance...

# TODO try vec_c(earlier, earlier, later) and vec_count-ing?

# TODO investigate https://github.com/r-lib/vctrs/blob/78d9f2b0b24131b5ce2230eb3c2c9f93620b10d9/bench/sorting-vs-hashing.md

## nested_groups <- .x$DT[, list(data = list(.SD)), keyby = geo_value]

## lapply(seq_len(nrow(nested_groups)), function(group_i) {
##   group_updates <- nested_groups$data[[group_i]][, list(subtbl = list(.SD)), keyby = version]
##   group_subresult <- epix_epi_slide_sub(
##     group_updates,
##     in_colnames,
##     .f,
##     before,
##     after,
##     time_type,
##     out_colnames)
## }) %>%
##   {vec_rbind(!!!.)}

map_accumulate_ea3 <- function(.x, .f, ...,
                               .init,
                               .f2_format = c("snapshot", "update"),
                               .clobberable_versions_start = NA,
                               .versions_end = NULL,
                               .compactify_abs_tol = 0,
                               .progress = FALSE) {
  # FIXME TODO

  if (length(.x) == 0L) {
    cli_abort("`.x` must have positive length")
  }

  .f <- as_mapper(.f)
  .f2_format <- arg_match(.f2_format)

  previous_accumulator <- .init
  other_keys <- NULL
  previous_version <- NULL
  previous_snapshot <- NULL
  diffs <- map(.x, .progress = .progress, .f = function(.x_entry) {
    .f_output <- .f(previous_accumulator, .x_entry, ...)

    if (!(is.list(.f_output) && length(.f_output) == 2L)) {
      cli_abort("`.f` must output a list of length 2 (new accumulator value
                 followed by a snapshot/update)")
    }
    .f_output2 <- .f_output[[2L]]

    if (is_epi_df(.f_output2)) {
      version <- attr(.f_output2, "metadata")[["as_of"]]
    } else {
      cli_abort('`.f` produced
        {c("snapshot" = "a snapshot", "update" = "an update")[[.f2_format]]}
        of an unsupported class:
        {epiprocess:::format_chr_deparse(class(.f_output2))}
      ')
    }

    if (!is.null(previous_version) && previous_version >= version) {
      # XXX this could give a very delayed error on unsorted versions. Go back
      # to requiring .x to be a version list and validate that?
      #
      # epi_diff2 also would validate this, but give a better message:
      cli_abort(c("Snapshots must be generated in ascending version order.",
                  "x" = "Version {previous_version} was followed by {version}.",
                  ">" = "If `.x` was a vector of version dates/tags, you might
                         just need to `sort` it."
                  ))
    }

    # Calculate diff with epikeytimeversion + value columns; we'll
    if (is.null(previous_snapshot)) {
      other_keys <<- attr(.f_output2, "metadata")[["other_keys"]]
      diff <- .f_output2
    } else {
      diff <- epi_diff2(previous_snapshot, .f_output2,
                        input_format = .f2_format,
                        compactify_abs_tol = .compactify_abs_tol)
    }

    # We'll need to diff any following outputs against an actual snapshot:
    snapshot <-
      if (.f2_format == "snapshot") {
        .f_output2
      } else { # .f2_format == "update"
        if (is.null(previous_snapshot)) {
          .f_output2
        } else {
          epi_patch(previous_snapshot, .f_output2)
        }
      }

    previous_accumulator <<- .f_output[[1L]]
    previous_version <<- version
    previous_snapshot <<- snapshot

    new_tibble(list(
      version = version,
      diff = list(as_tibble(diff))
    ))
  })

  # More validation&defaults we can only do now:
  if (is.null(.versions_end)) {
    .versions_end <- previous_version
  } else if (.versions_end < previous_version) {
    cli_abort(c(
      "Specified `.versions_end` was earlier than the final `as_of`.",
      "*" = "`.versions_end`: {.versions_end}",
      "*" = "Final `as_of`: {previous_version}"
    ))
  }

  # rbindlist sometimes is a little fast&loose with attributes; use
  # vec_rbind/bind_rows/unnest and convert:
  diffs <- unnest(vec_rbind(!!!diffs), diff, names_sep = NULL)
  # `unpack()` might possibly alias a diff if all others (if any) are empty, and
  # diffs might alias inputs. So use as.data.table rather than setDT;
  # performance-wise it doesn't seem to really matter.
  diffs <- as.data.table(diffs, key = c("geo_value", other_keys, "time_value", "version"))
  setcolorder(diffs) # default: key first, then value cols

  diffs <- as_epi_archive(
    diffs,
    other_keys = other_keys,
    clobberable_versions_start = .clobberable_versions_start,
    versions_end = .versions_end,
    compactify = FALSE # we already compactified; don't re-do work or change tol
  )

  list(previous_accumulator, diffs)
}

# TODO check for omitted check_ukey_unique checks
