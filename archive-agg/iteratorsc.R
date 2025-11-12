
# variant of itrb hoping for coro compatibility by having default
# indices that would advance things, though coro might check for 0
# args and/or have some special type expecting to just feed in
# something else as first arg...

# Iterators allow us to perform some reduce operations without requiring as much
# maximum RAM holding large lists, and to provide more immediate error messaging
# when a series of maps etc. contains an error that triggers consistently,
# rather than computing all of a costly earlier map operation, only to raise an
# error on the first try of a subsequent map or reduce operation (either due to
# a bug there, or validation there that detects a bug in the early operations
# done by the prior maps). This early-error behavior may let us abstract away
# some validation and munging that is mixed with the main logic of some slide
# functions, assuming iterator performance is sufficient.

new_epi_sized_iteratorc <- function(size, f) {
  assert_int(size, lower = 0)
  assert_function(f, args = "i")
  # TODO validate auto-inc?
  assert_false(inherits(f, "epi_sized_iteratorc"))

  itrc <- f
  attr(itrc, "epiprocess::size") <- size
  class(itrc) <- "epi_sized_iteratorc"
  itrc
}

# If the previously-requested index was iprev, then f(iprev) and f(iprev + 1L)
# must both work; other indices may work but are not required to. This approach
# allows us to have a map2-like operation that depends twice on an iterator,
# either directly or indirectly, without accidentally double-advancing the
# upstream iterator; sized-iterators catches this with the start/claim
# mechanism; bare coro iterators don't catch this. It also potentially helps
# with debugging; when recover()-ing, we can step through more kinds of iterator
# code without accidentally advancing the iterator.

itrc_size <- function(itrc) {
  attr(itrc, "epiprocess::size")
}

# TODO rename? input doesn't have to be list
list_itrc <- function(x) {
  new_epi_sized_iteratorc(
    length(x),
    function(i) x[[i]]
  )
}

# This one only returns strictly what's required by the itrc approach. Perhaps
# good for debugging iterator issues by being more rigid.
list_streamcache_itrc <- function(x) {
  curr_i <- 0L
  curr_result <- NULL
  new_epi_sized_iteratorc(
    length(x),
    function(i = curr_i + 1L) {
      if (i == curr_i) {
        curr_result
      } else if (i == curr_i + 1L) {
        result <- x[[i]]
        # FIXME atomicity
        curr_i <<- i
        curr_result <<- result
        curr_result
      } else {
        stop("can only get current value or move on to next value")
      }
    }
  )
}

as.list.epi_sized_iteratorc <- function(itrc) {
  result <- vector("list", attr(itrc, "epiprocess::size"))
  for (i in seq_len(attr(itrc, "epiprocess::size"))) {
    result[[i]] <- itrc(i)
  }
  result
}

itrc_map_itrc <- function(itrc, f) {
  curr_state <- list(0L, NULL) # i, result
  new_epi_sized_iteratorc(
    itrc_size(itrc),
    function(i = curr_state[[1L]] + 1L) {
      curr_i <- curr_state[[1L]]
      if (i == curr_i) {
        if (curr_i == 0L) {
          stop("0 out of bounds")
        } else {
          curr_state[[2L]]
        }
      } else {
        result <- f(itrc(i))
        curr_state <- list(i, result) # atomic update
        result
      }
    }
  )
}

obj_itrc_reduce <- function(init, itrc, f2) {
  res <- init
  for (i in seq_len(itrc_size(itrc))) {
    res <- f2(res, itrc(i))
  }
  res
}

list_itrc(1:5) %>% as.list()
list_itrc(1:5) %>% itrc_map_itrc(function(x) x^2) %>% as.list()

ic1 <- list_itrc(1:5) %>% itrc_map_itrc(function(x) x^2)
ic1(1)
ic1(1)
ic1(2)
ic1(4)
ic1(1)

ic2 <- list_streamcache_itrc(1:5) %>% itrc_map_itrc(function(x) x^2)
ic2(1)
ic2(1)
ic2(2)
ic2(4)
ic2(1)

list_itrc(1:5) %>% itrc_map_itrc(function(x) x^2) %>% obj_itrc_reduce(init = 0, sum)

# TODO apply perf checking for common case of advancing i first




