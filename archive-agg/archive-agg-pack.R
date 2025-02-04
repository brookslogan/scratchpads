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
  # Most input validation:
  assert_class(earlier_edf, "epi_df")
  assert_class(later_edf, "epi_df")
  input_format <- arg_match(input_format)
  assert_numeric(compactify_tol, lower = 0, any.missing = FALSE, len = 1)

  # Extract metadata:
  earlier_version <- attr(earlier_edf, "metadata")$as_of
  later_metadata <- attr(later_edf, "metadata")
  other_keys <- later_metadata$other_keys
  later_version <- later_metadata$as_of
  edf_names <- names(later_edf)
  ekt_names <- c("geo_value", other_keys, "time_value")
  val_names <- edf_names[! edf_names %in% ekt_names]
  earlier_n <- nrow(earlier_edf)
  later_n <- nrow(later_edf)

  # More input validation:
  if (earlier_version >= later_version) {
    cli_abort(c("`later_edf` should have a later as_of than `earlier_edf`",
                "i" = "`earlier_edf`'s as_of: {earlier_version}",
                "i" = "`later_edf`'s as_of: {later_version}"))
  }

  # Convert to tibble so we won't violate (planned) `epi_df` key-uniqueness
  # invariants by combining (which we need for more efficient processing):
  earlier_tbl <- as_tibble(earlier_edf)
  later_tbl <- as_tibble(later_edf)
  combined_tbl <- vec_c(earlier_tbl, later_tbl)
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
    combined_tbl <- combined_tbl[combined_from_later & !combined_compactify_away, ]
  } else { # input_format == "snapshot"
    # Which rows from combined are in case 1.?
    combined_is_deletion <- vec_rep_each(c(TRUE, FALSE), c(earlier_n, later_n))
    combined_is_deletion[ekt_repeat_first_i] <- FALSE

    # Which rows from combined are in cases 1., 4., or 5.?
    combined_include <- combined_is_deletion | combined_from_later & !combined_compactify_away
    combined_tbl <- combined_tbl[combined_include, ]
    # Represent deletion in 1. with NA-ing of all value columns. In some previous
    # approaches to epi_diff2, this seemed to be faster than using
    # vec_c(case_1_ekts, cases_45_tbl) or bind_rows to fill with NAs, and more
    # general than data.table's rbind(case_1_ekts, cases_45_tbl, fill = TRUE):
    combined_tbl[combined_is_deletion[combined_include], val_names] <- NA
  }

  # XXX the version should probably be an attr at this point; this is for
  # compatibility with some other epi_diff2 variants being tested
  combined_tbl$version <- later_version

  combined_tbl <- as.data.table(combined_tbl)

  combined_tbl
}

epi_diff2(
  as_epi_df(tibble(geo_value = 1, time_value = 1:3, value = 1:3),
            as_of = 5),
  as_epi_df(tibble(geo_value = 1, time_value = 2:4, value = c(2, 5, 6)),
            as_of = 6)
)

epi_diff2(
  as_epi_df(tibble(geo_value = 1, time_value = 1:3, value = 1:3),
            as_of = 5),
  as_epi_df(tibble(geo_value = 1, time_value = 2:4, value = c(2, 5, 6)),
            as_of = 6),
  input_format = "update"
)
