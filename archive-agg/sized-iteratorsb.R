
# Iterators allow us to perform some reduce operations without requiring as much
# maximum RAM holding large lists, and to provide more immediate error messaging
# when a series of maps etc. contains an error that triggers consistently,
# rather than computing all of a costly earlier map operation, only to raise an
# error on the first try of a subsequent map or reduce operation (either due to
# a bug there, or validation there that detects a bug in the early operations
# done by the prior maps). This early-error behavior may let us abstract away
# some validation and munging that is mixed with the main logic of some slide
# functions, assuming iterator performance is sufficient.

new_epi_sized_iteratorb <- function(size, f) {
  assert_int(size, lower = 0)
  assert_function(f, args = "i")
  assert_false(inherits(f, "epi_sized_iteratorb"))

  itrb <- f
  attr(itrb, "epiprocess::size") <- size
  class(itrb) <- "epi_sized_iteratorb"
  itrb
}

# If the previously-requested index was iprev, then f(iprev) and f(iprev + 1L)
# must both work; other indices may work but are not required to. This approach
# allows us to have a map2-like operation that depends twice on an iterator,
# either directly or indirectly, without accidentally double-advancing the
# upstream iterator; sized-iterators catches this with the start/claim
# mechanism; bare coro iterators don't catch this. It also potentially helps
# with debugging; when recover()-ing, we can step through more kinds of iterator
# code without accidentally advancing the iterator.

itrb_size <- function(itrb) {
  attr(itrb, "epiprocess::size")
}

# TODO rename? input doesn't have to be list
list_itrb <- function(x) {
  new_epi_sized_iteratorb(
    length(x),
    function(i) x[[i]]
  )
}

# This one only returns strictly what's required by the itrb approach. Perhaps
# good for debugging iterator issues by being more rigid.
list_streamcache_itrb <- function(x) {
  curr_i <- 0L
  curr_result <- NULL
  new_epi_sized_iteratorb(
    length(x),
    function(i) {
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

as.list.epi_sized_iteratorb <- function(itrb) {
  result <- vector("list", attr(itrb, "epiprocess::size"))
  for (i in seq_len(attr(itrb, "epiprocess::size"))) {
    result[[i]] <- itrb(i)
  }
  result
}

itrb_map_itrb <- function(itrb, f) {
  curr_state <- list(0L, NULL) # i, result
  new_epi_sized_iteratorb(
    itrb_size(itrb),
    function(i) {
      curr_i <- curr_state[[1L]]
      if (i == curr_i) {
        if (curr_i == 0L) {
          stop("0 out of bounds")
        } else {
          curr_state[[2L]]
        }
      } else {
        result <- f(itrb(i))
        curr_state <- list(i, result) # atomic update
        result
      }
    }
  )
}

obj_itrb_reduce <- function(init, itrb, f2) {
  res <- init
  for (i in seq_len(itrb_size(itrb))) {
    res <- f2(res, itrb(i))
  }
  res
}

list_itrb(1:5) %>% as.list()
list_itrb(1:5) %>% itrb_map_itrb(function(x) x^2) %>% as.list()

ib1 <- list_itrb(1:5) %>% itrb_map_itrb(function(x) x^2)
ib1(1)
ib1(1)
ib1(2)
ib1(4)
ib1(1)

ib2 <- list_streamcache_itrb(1:5) %>% itrb_map_itrb(function(x) x^2)
ib2(1)
ib2(1)
ib2(2)
ib2(4)
ib2(1)

list_itrb(1:5) %>% itrb_map_itrb(function(x) x^2) %>% obj_itrb_reduce(init = 0, sum)

# TODO apply perf checking for common case of advancing i first
