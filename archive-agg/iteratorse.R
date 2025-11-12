# itre with itr_for_each in internals and setup for combining with itrd

new_epi_sized_iteratore <- function(size, f) {
  assert_int(size, lower = 0)
  assert_function(f, args = "i")
  assert_false(inherits(f, "epi_sized_iteratore"))

  itre <- f
  attr(itre, "epiprocess::size") <- size
  class(itre) <- "epi_sized_iteratore"
  itre
}

# If the previously-requested index was iprev, then f(iprev) and f(iprev + 1L)
# must both work; other indices may work but are not required to. This approach
# allows us to have a map2-like operation that depends twice on an iterator,
# either directly or indirectly, without accidentally double-advancing the
# upstream iterator; sized-iterators catches this with the start/claim
# mechanism; bare coro iterators don't catch this. It also potentially helps
# with debugging; when recover()-ing, we can step through more kinds of iterator
# code without accidentally advancing the iterator.

itre_size <- function(itre) {
  attr(itre, "epiprocess::size")
}

# TODO rename? input doesn't have to be list
list_itre <- function(x) {
  new_epi_sized_iteratore(
    length(x),
    function(i) x[[i]]
  )
}

itre_for_each <- function(itre, f) UseMethod("itre_for_each")

itre_for_each.epi_sized_iteratore <- function(itre, f) {
  for (i in seq_len(attr(itre, "epiprocess::size"))) {
    f(itre(i))
  }
}

as.list.epi_sized_iteratore <- function(itre) {
  result <- vector("list", attr(itre, "epiprocess::size"))
  for (i in seq_len(attr(itre, "epiprocess::size"))) {
    result[[i]] <- itre(i)
  }
  result
}

itre_map_itre <- function(itre, f) {
  curr_state <- list(0L, NULL) # i, result
  size <- attr(itre, "epiprocess::size", exact = TRUE)
  g <- function(i) {
    curr_i <- curr_state[[1L]]
    if (i == curr_i) {
      if (curr_i == 0L) {
        stop("0 out of bounds")
      } else {
        curr_state[[2L]]
      }
    } else {
      inp <- itre(i)
      # XXX paying perf here, but seems like would have to have two
      # function bodies or error approach to avoid... or maybe a
      # require-at-least-one-element-and-set-size-when-get-to-last-and-expect-that-to-be-checked-by-something-and-avoid-querying-beyond... or
      # call a stopper function provided by any consumers that will,
      # e.g., replace some helper function with another? or set some
      # stop flag instead of the size, eliminating a call to
      # identical?
      result <- if (identical(inp, cached_exhausted)) cached_exhausted else f(inp)
      curr_state <- list(i, result) # atomic update
      result
    }
  }
  if (is.null(size)) {
    stop("todo")
  } else {
    new_epi_sized_iteratore(size, g)
  }
}

# perhaps itrs can be defined like abstract class requiring a map as
# the core basic operation?  for_each didn't actually seem useful in
# impl...  not sure what else might need.  fold and for_each likely
# can be defined in terms of map. collect could be specialized for
# sized things but there could be a default

obj_itre_reduce <- function(init, itre, f2) {
  res <- init
  for (i in seq_len(itre_size(itre))) {
    res <- f2(res, itre(i))
  }
  res
}

list_itre(1:5) %>% as.list()
list_itre(1:5) %>% itre_map_itre(function(x) x^2) %>% as.list()

ie1 <- list_itre(1:5) %>% itre_map_itre(function(x) x^2)
ie1(1)
ie1(1)
ie1(2)
ie1(4)
ie1(1)

# ie2 <- list_streamcache_itre(1:5) %>% itre_map_itre(function(x) x^2)
# ie2(1)
# ie2(1)
# ie2(2)
# ie2(4)
# ie2(1)

list_itre(1:5) %>% itre_map_itre(function(x) x^2) %>% obj_itre_reduce(init = 0, sum)

# TODO apply perf checking for common case of advancing i first

# XXX additional class on itr_map_itr could help with other perf ideas like turning %>% imi(f) %>% imi(g) into equivalent of %>% imi(g.f).
