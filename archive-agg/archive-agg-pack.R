library(epidatr)
library(epiprocess)
library(dplyr)
library(tidyr)
library(data.table)
library(purrr)
library(vctrs)
library(checkmate)
library(rlang)

snapshots <- epidatasets::archive_cases_dv_subset %>%
  epix_slide(~ list(.x))
fewer_snapshots <- head(snapshots, n = 200)

approx_equal <- function(vec1, vec2, abs_tol, na_equal, .ptype = NULL, recurse = approx_equal, inds1 = NULL, inds2 = NULL) {
  vecs <- list(vec1, vec2)
  if (!is.null(inds1)) {
    # XXX could have logical or integerish inds; just leave it to later checks
    # to hopefully abort for now
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


epi_diff2 <- function(earlier_edf, later_edf,
                      input_format = c("snapshot", "update"),
                      compactify_tol = 0) {
  # Most input validation. This is a small function so use faster validation
  # variants:
  if (!inherits(earlier_edf, "epi_df")) {
    cli_abort("`earlier_edf` must be an `epi_df`")
  }
  if (!inherits(later_edf, "epi_df")) {
    cli_abort("`later_edf` must be an `epi_df`")
  }
  input_format <- arg_match0(input_format, c("snapshot", "update"))
  if (!(is.vector(compactify_tol, mode = "numeric") && length(compactify_tol) == 1 && compactify_tol >= 0)) {
    # Give a specific message:
    assert_numeric(compactify_tol, lower = 0, any.missing = FALSE, len = 1)
    # Fallback e.g. for invalid classes not caught by assert_numeric:
    cli_abort("`compactify_tol` must be a length-1 double/integer >= 0")
  }

  # Extract metadata:
  earlier_metadata <- attr(earlier_edf, "metadata")
  earlier_version <- earlier_metadata[["as_of"]]
  earlier_n <- nrow(earlier_edf)

  later_metadata <- attr(later_edf, "metadata")
  later_version <- later_metadata[["as_of"]]
  later_n <- nrow(later_edf)

  other_keys <- earlier_metadata[["other_keys"]]
  edf_names <- names(earlier_edf)
  ekt_names <- c("geo_value", other_keys, "time_value")
  val_names <- edf_names[! edf_names %in% ekt_names]

  # TODO validate other metadata matching.  maybe just require metadata list identical aside from as_of

  # TODO check no `version colname`

  # More input validation:
  if (!identical(edf_names, names(later_edf))) {
    # XXX is this check actually necessary?
    cli_abort(c("`earlier_edf` and `later_edf` should have identical column
                 names and ordering.",
                "*" = "`earlier_edf` colnames: {format_chr_deparse(edf_names)}",
                "*" = "`later_edf` colnames: {format_chr_deparse(names(later_edf))}"))
  }
  if (!identical(other_keys, later_metadata[["other_keys"]])) {
    cli_abort(c("`earlier_edf` and `later_edf` should have identical other_keys.",
                "*" = "`earlier_edf` other_keys: {format_chr_deparse(other_keys)}",
                "*" = '`later_edf` other_keys: {format_chr_deparse(later_metadata[["other_keys"]])}'))
  }
  if (earlier_version >= later_version) {
    cli_abort(c("`later_edf` should have a later as_of than `earlier_edf`",
                "i" = "`earlier_edf`'s as_of: {earlier_version}",
                "i" = "`later_edf`'s as_of: {later_version}"))
  }

  # Convert to tibble so we won't violate (planned) `epi_df` key-uniqueness
  # invariants by `vec_rbind`ing them (which we use for efficient processing using
  # hash-table-based duplicate detection, without relying on DTthreads).
  earlier_tbl <- as_tibble(earlier_edf)
  later_tbl <- as_tibble(later_edf)
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

  # For "snapshot" input_format, we need to filter to 1., 4., and 5., and alter
  # values for 1.  For "update" input_format, we need to filter to 4. and 5.

  # (For compactify_tol = 0, we could potentially streamline things by dropping
  # ekt+val duplicates (cases 2. and 3.).)

  # Row indices of first occurrence of each ekt; will be the same as
  # seq_len(combined_n) except for when that ekt has been re-reported in
  # `later_edf`, in which case (3. or 4.) it will point back to the row index of
  # the same ekt in `earlier_edf`:
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
                  abs_tol = compactify_tol,
                  na_equal = TRUE,
                  inds1 = combined_ekt_is_repeat,
                  inds2 = ekt_repeat_first_i)

  # Which rows from combined are in cases 3., 4., or 5.?
  combined_from_later <- vec_rep_each(c(FALSE, TRUE), c(earlier_n, later_n))

  if (input_format == "update") {
    # Cases 4. and 5.:
    combined_tbl <- combined_tbl[combined_from_later & !combined_compactify_away, ]
  } else { # input_format == "snapshot"
    # Which rows from combined are in case 1.?
    combined_is_deletion <- vec_rep_each(c(TRUE, FALSE), c(earlier_n, later_n))
    combined_is_deletion[ekt_repeat_first_i] <- FALSE
    # Which rows from combined are in cases 1., 4., or 5.?
    combined_include <- combined_is_deletion | combined_from_later & !combined_compactify_away
    combined_tbl <- combined_tbl[combined_include, ]
    # Represent deletion in 1. with NA-ing of all value columns. (In some
    # previous approaches to epi_diff2, this seemed to be faster than using
    # vec_rbind(case_1_ekts, cases_45_tbl) or bind_rows to fill with NAs, and more
    # general than data.table's rbind(case_1_ekts, cases_45_tbl, fill = TRUE).)
    combined_tbl[combined_is_deletion[combined_include], val_names] <- NA
  }

  reclass(combined_tbl, later_metadata)
}

epi_patch <- function(snapshot, update) {
  # Most input validation. This is a small function so use faster validation
  # variants:
  if (!inherits(snapshot, "epi_df")) {
    cli_abort("`snapshot` must be an `epi_df`")
  }
  if (!inherits(update, "epi_df")) {
    # XXX debating about whether to have a specialized class for updates/diffs.
    # Seems nice for type-based reasoning and might remove some args from
    # interfaces, but would require constructor/converter functions for that
    # type.
    cli_abort("`update` must be an `epi_df`")
  }
  snapshot_metadata <- attr(snapshot, "metadata")
  update_metadata <- attr(update, "metadata")
  snapshot_version <- snapshot_metadata$as_of
  update_version <- update_metadata$as_of
  snapshot_other_metadata <- snapshot_metadata[names(snapshot_metadata) != "as_of"]
  update_other_metadata <- update_metadata[names(update_metadata) != "as_of"]
  if (!identical(snapshot_other_metadata, update_other_metadata)) {
    # TODO refactor this into a common (ptype?) check?
    cli_abort(
      "Incompatible `snapshot` and `update` metadata:",
      body = capture.output(waldo::compare(
        snapshot_other_metadata, update_other_metadata,
        x_arg = "snapshot_metadata", y_arg = "update_metadata"
      ))
    )
  }
  if (snapshot_version >= update_version) {
    cli_abort(c("`update` should have a later as_of than `snapshot`",
                "i" = "`snapshot`'s as_of: {snapshot_version}",
                "i" = "`update`'s as_of: {update_version}"))
  }

  ekt_names <- c("geo_value", snapshot_metadata$other_keys, "time_value")

  result_tbl <- vec_rbind(as_tibble(update), as_tibble(snapshot))

  dup_ids <- vec_duplicate_id(result_tbl[ekt_names])
  not_overwritten <- dup_ids == vec_seq_along(result_tbl)
  result_tbl <- result_tbl[not_overwritten,]

  result_tbl <- reclass(result_tbl, update_metadata)
  result_tbl <- arrange_canonical(result_tbl)

  result_tbl
}

map_ea <- function(.x, .f, ...,
                   .f_format = c("snapshot", "update"),
                   .clobberable_versions_start = NA,
                   .compactify_tol = 0,
                   .progress = FALSE) {
  if (length(.x) == 0L) {
    cli_abort("`.x` must have positive length")
  }

  .f <- as_mapper(.f)
  .f_format <- arg_match(.f_format)

  other_keys <- NULL
  previous_version <- NULL
  previous_snapshot <- NULL
  diffs <- map(.x, .progress = .progress, .f = function(.x_entry) {
    .f_output <- .f(.x_entry, ...)
    if (is_epi_df(.f_output)) {
      version <- attr(.f_output, "metadata")[["as_of"]]
    } else {
      cli_abort("`.f` produced an unsupported class:
                 {epiprocess:::format_chr_deparse(class(.f_output))}")
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
      other_keys <<- attr(.f_output, "metadata")[["other_keys"]]
      diff <- .f_output
    } else {
      diff <- epi_diff2(previous_snapshot, .f_output,
                        input_format = .f_format,
                        compactify_tol = .compactify_tol)
    }

    # We'll need to diff any following outputs against an actual snapshot:
    snapshot <-
      if (.f_format == "snapshot") {
        .f_output
      } else { # .f_format == "update"
        if (is.null(previous_snapshot)) {
          .f_output
        } else {
          epi_patch(previous_snapshot, .f_output)
        }
      }

    previous_version <<- version
    previous_snapshot <<- snapshot

    new_tibble(list(
      version = version,
      diff = list(as_tibble(diff))
    ))
  })

  # rbindlist sometimes is a little fast&loose with attributes; use
  # vec_rbind/bind_rows/unnest and convert:
  diffs <- unnest(vec_rbind(!!!diffs), diff, names_sep = NULL)
  # `unpack()` might possibly alias a diff if all others (if any) are empty, and
  # diffs might alias inputs. So use as.data.table rather than setDT;
  # performance-wise it doesn't seem to really matter.
  diffs <- as.data.table(diffs, key = c("geo_value", other_keys, "time_value", "version"))
  setcolorder(diffs) # default: key first, then value cols

  as_epi_archive(
    diffs,
    other_keys = other_keys,
    clobberable_versions_start = .clobberable_versions_start,
    versions_end = previous_version,
    compactify = FALSE # we already compactified; don't re-do work or change tol
  )
}

edf1 <- as_epi_df(tibble(geo_value = 1, time_value = 1:3, value = 1:3),
                  as_of = 5L)
edf2 <- as_epi_df(tibble(geo_value = 1, time_value = 2:4, value = c(2, 5, 6)),
                  as_of = 6L)

# TODO check time_value-version compatibility ahead of time.

epi_diff2(edf1, edf2)

epi_diff2(edf1, edf2, input_format = "update")

epi_patch(edf1, epi_diff2(edf1, edf2))

epi_patch(edf1, epi_diff2(edf1, edf2)) %>%
  filter(!is.na(value)) %>%
  waldo::compare(edf2)

map_ea(snapshots$slide_value, identity)

map_ea(list(edf1, edf2), identity)

map_ea(list(edf1, edf2), identity, .f_format = "update")

# TODO reconsider terminology... "update" vs. "patch" vs. "diff", etc.; want
# something that applies to
# something-that-is-either-snapshot-or-update/patch/diff, to not suggest
# compact/minimal-diffs if not compact/minimal, and to not clobber potential
# class constructors with functions that work with / generate those objects in
# another way
