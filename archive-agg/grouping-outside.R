

#' Simple alternative to proper `group_map.epi_archive`
#'
#' The main difference is that we're not directly feeding in "extracts" from the
#' archive because we haven't implemented a good class for when these extracts
#' drop the `geo_value` column (when `geo_value` is in the group columns and
#' `.keep = FALSE`). Instead of implementing a proper class for these types of
#' extracts and feeding it or epi_archives to computations depending on the
#' grouping, we'll just feed in something similar but without a class and
#' surrounding dressings.
#'
#' Minor differences: we require `.x` to be manually grouped rather than
#' inferring a trivial grouping for ungrouped archives, and assume `.keep =
#' FALSE`. We ignore $drop.
gea_group_map <- function(.x, .f, ...) {
  assert_class(.x, "grouped_epi_archive")
  .f <- as_mapper(.f)

  groups <- .x$private$ungrouped$DT[, list(.data = list(.SD)), keyby = .x$private$vars]
  lapply(seq_len(nrow(groups)), function(group_i) {
    stop("FIXME TODO")
  })
}

# XXX vs. group_map on the DT and pass another arg or set attributes to communicate other info?

gea_group_map2 <- function(.x, .f, ...) {
  assert_class(.x, "grouped_epi_archive")
  assert_function(.f) # todo arity

  .x$private$ungrouped$DT %>%
    as.data.frame() %>%
    as_tibble() %>%
    group_by(pick(all_of(.x$private$vars)), .drop = .x$private$drop) %>%
    group_map(
      function(grp_data, grp_key) {
        .f(grp_data, grp_key, as.list(archive_cases_dv_subset)[-1], ...)
      }
    )
}

gea_group_split <- function(.x) {
  assert_class(.x, "grouped_epi_archive")
  groups <- .x$private$ungrouped$DT[, list(.data = list(.SD)), keyby = .x$private$vars]
  lapply(seq_len(nrow(groups)), function(group_i) {
    stop("FIXME TODO")
  })
}



## bench::mark(
##   archive_cases_dv_subset$DT[, list(.data = list(.SD)), keyby = "geo_value"],
##   nest(archive_cases_dv_subset$DT, .by = geo_value),
##   group_split(archive_cases_dv_subset$DT %>% group_by(geo_value)),
##   vec_split(archive_cases_dv_subset$DT, archive_cases_dv_subset$DT$geo_value),
##   check = FALSE
## )

## TODO impl group_data first/instead?

## TODO vs. itr approaches, to allow combinators with early/midway combination validation failures


## tbl from .drop = FALSE example

## tbl %>%
##   nest(.by = c(x, y)) %>%
##   group_by(x, y, .drop = FALSE) %>%
##   group_split() %>%
##   {}

# Something to try to simplify away .drop = FALSE...:

tbl2 <- tbl %>%
  mutate(z = seq_len(nrow(.)))

tbl2 %>%
  nest(.by = c(x, y), .key = ".grp_data") %>%
  ## mutate(.grp_data = list_of(!!!.grp_data, .ptype = .env$tbl[0, !names(.env$tbl) %in% c("x", "y")])) %>%
  ## mutate(.grp_data = as_list_of(.grp_data)) %>%
  ## mutate(.grp_data = {grp_data <- .grp_data; list_of(!!!grp_data)}) %>%
  ## `[[<-`(".grp_data", value = list_of(!!!.$.grp_data, .ptype = tbl[0, !names(tbl) %in% c("x", "y")])) %>%
  {
    complete_backrefs <- group_data(group_by(., x, y, .drop = FALSE))
    complete_grp_data <- vector("list", nrow(complete_backrefs))
    # XXX maybe could do something with vec_chop & fixup empty lists
    complete_grp_was_present <- list_sizes(complete_backrefs$.rows) != 0L
    present_row_is <- list_unchop(complete_backrefs$.rows)
    complete_grp_data[complete_grp_was_present] <- .$.grp_data[present_row_is]
    complete_grp_data[!complete_grp_was_present] <- list(tbl2[0, ! names(tbl2) %in% c("x", "y")])
    complete_nesting <- complete_backrefs
    complete_nesting$.rows <- NULL
    complete_nesting$.grp_data <- complete_grp_data
    complete_nesting
  } %>%
  {}

# TODO in vec_split as_tibble format?

## dtbl_group_nest <- function(dtbl, .......)



# NOTE data.table "nest" operation seems to setkey only on outside, not mark the .SDs with the setdiff of the key & group vars; thought it did before...





group_nest <- function(x) {
  grp_vars <- group_vars(x)
  grouped_nested <- nest(x, .key = ".grp_data") # doesn't expand for .drop = FALSE
  # TODO perf: short-circuit below if no .drop = FALSE
  complete_backrefs <- group_data(grouped_nested)
  attr(complete_backrefs, ".drop") <- NULL
  complete_grp_data <- vector("list", nrow(complete_backrefs))
  # XXX maybe could do something with vec_chop & fixup empty lists
  complete_grp_was_present <- list_sizes(complete_backrefs$.rows) != 0L
  present_row_is <- list_unchop(complete_backrefs$.rows)
  complete_grp_data[complete_grp_was_present] <- grouped_nested$.grp_data[present_row_is]
  complete_grp_data[!complete_grp_was_present] <- list(x[0, ! names(x) %in% grp_vars])
  complete_nested <- complete_backrefs
  complete_nested$.rows <- NULL
  complete_nested$.grp_data <- complete_grp_data
  complete_nested
}

drop_test_tbl <- tibble(a = factor(letters[c(1,1,2)], letters[1:5]), b = c(1,1,1), v = list(1:5,1:2,1))
bench::mark(
  drop_test_tbl %>% group_by(b, a, .drop = FALSE) %>% group_nest(),
  drop_test_tbl  %>% group_by(b, a, .drop = FALSE) %>% group_map(~ .y %>% mutate(.grp_data = list(.x))) %>% bind_rows(),
  drop_test_tbl  %>% group_by(b, a, .drop = FALSE) %>% group_map(function(.x, .y) .y %>% mutate(.grp_data = list(.x))) %>% bind_rows(),
  drop_test_tbl  %>% group_by(b, a, .drop = FALSE) %>% group_modify(~ tibble(.grp_data = list(.x))) %>% ungroup()
)
# XXX looks like these simpler approaches are faster... but dplyr nesting seemed slow (at some point??), so that may mean that they are all slow...


epix_group_split_itrb <- function(.x) {
  assert_class(.x, "grouped_epi_archive")

  .x$private$ungrouped$DT %>%
    as.data.frame() %>%
    as_tibble() %>%
    group_by(pick(all_of(.x$private$vars)), .drop = .x$private$drop) %>%
    group_split() %>%
    list_itrb()
}

archive_cases_dv_subset %>%
  group_by(geo_value) %>%
  epix_group_split_itrb() %>%
  as.list()

epix_group_itrb <- function(.x) {
  assert_class(.x, "grouped_epi_archive")

  .x$private$ungrouped$DT %>%
    as.data.frame() %>%
    as_tibble() %>%
    group_by(pick(all_of(.x$private$vars)), .drop = .x$private$drop) %>%
    group_map(
      function(group_values, group_key) list(group_values = group_values, group_key = group_key)
    ) %>%
    list_itrb()
}

archive_cases_dv_subset %>%
  group_by(geo_value) %>%
  epix_group_itrb() %>%
  as.list()

## group_itrb_combine <- function(group_itrb) {
##   group_itrb %>%
##     itrb_map(function(group) {
##       stop("TODO dedupe logic, maybe df/vec handling, ...")
##     })
## }

group_itrb1 <- function(.data) {
  .data %>%
    group_map(
      # group_map validation doesn't allow for just `list`; forward:
      function(group_values, group_key) list(group_values, group_key)
    ) %>%
    list_itrb()
}

group_itrb2 <- function(.data) {
  .data %>%
    group_map(
      # group_map validation doesn't allow for just `list`; forward:
      function(group_values, group_key) list(group_values = group_values, group_key = group_key)
    ) %>%
    list_itrb()
}

group_modify_alt1 <- function(.data, .f, ..., .group_itrb, .keep = FALSE) {
  .f <- as_mapper(.f)
  group_itrb <- .group_itrb(.data)
  result_itrb <- group_itrb %>%
    itrb_map_itrb(function(group) {
      subres <- .f(group[[1L]], group[[2L]], ...)
    })
  group_key_names <- group_vars(.data)
  checked_itrb <- result_itrb %>%
    itrb_map_itrb(function(group_output) {
      if (!is.data.frame(group_output)) {
        stop("group_output needs to be data.frame")
      }
      if (any(names(group_output) %in% group_key_names)) {
        stop("output column with group var name")
      }
      group_output
    })
  group_labeled_outputs <-
    lapply(seq_len(itrb_size(group_itrb)), function(i) {
      group_key <- group_itrb(i)[[2L]]
      group_output <- checked_itrb(i)
      vec_cbind(group_key, group_output, .name_repair = "minimal")
    })
  vec_rbind(!!!group_labeled_outputs) %>%
    # XXX inefficient; could smartly construct:
    dplyr_reconstruct(.data)
}

# try accessing group_values & group_key fields by name with $
group_modify_alt2 <- function(.data, .f, ..., .keep = FALSE) {
  .f <- as_mapper(.f)
  group_itrb <- group_itrb2(.data)
  result_itrb <- group_itrb %>%
    itrb_map_itrb(function(group) {
      subres <- .f(group$group_values, group$group_key, ...) # *******
    })
  group_key_names <- group_vars(.data)
  checked_itrb <- result_itrb %>%
    itrb_map_itrb(function(group_output) {
      if (!is.data.frame(group_output)) {
        stop("group_output needs to be data.frame")
      }
      if (any(names(group_output) %in% group_key_names)) {
        stop("output column with group var name")
      }
      group_output
    })
  group_labeled_outputs <-
    lapply(seq_len(itrb_size(group_itrb)), function(i) {
      group_key <- group_itrb(i)[[2L]]
      group_output <- checked_itrb(i)
      vec_cbind(group_key, group_output, .name_repair = "minimal")
    })
  vec_rbind(!!!group_labeled_outputs) %>%
    # XXX inefficient; could smartly construct:
    dplyr_reconstruct(.data)
}

group_itrb <- group_itrb2
group_modify_alt <- partial(group_modify_alt1, .group_itrb = group_itrb2) # XXX overhead...

bench::mark(
  tibble(g = c(1,1,1,2,3), v = 1:5) %>% group_by(g) %>% group_modify(~ tibble(sum = sum(.x$v))),
  tibble(g = c(1,1,1,2,3), v = 1:5) %>% group_by(g) %>% group_modify_alt1(~ tibble(sum = sum(.x$v)), .group_itrb = group_itrb1),
  tibble(g = c(1,1,1,2,3), v = 1:5) %>% group_by(g) %>% group_modify_alt1(~ tibble(sum = sum(.x$v)), .group_itrb = group_itrb2),
  tibble(g = c(1,1,1,2,3), v = 1:5) %>% group_by(g) %>% group_modify_alt2(~ tibble(sum = sum(.x$v))),
  tibble(g = c(1,1,1,2,3), v = 1:5) %>% group_by(g) %>% group_modify_alt(~ tibble(sum = sum(.x$v))),
  min_time = 20
)
