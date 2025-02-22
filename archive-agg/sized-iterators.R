
new_epi_sized_iterator <- function(size, f) {
  assert_int(size, lower = 0)
  assert_function(f, args = character()) # assume coro-compliant iterator with no `close` arg
  assert_false(inherits(f, "epi_sized_iterator"))

  itr <- f
  attr(itr, "epiprocess::size") <- size
  environment(itr)[[".epiprocess::iterator_started"]] <- FALSE
  class(itr) <- "epi_sized_iterator"
  itr
}

# XXX or for debuggability should iterators be indexing into something but not have to keep around previous elements?  to prevent accidental advances.  even with start/claim there are limits

itr_size <- function(itr) {
  attr(itr, "epiprocess::size")
}

itr_start <- function(itr) {
  assert_class(itr, "epi_sized_iterator")
  env <- environment(itr)
  if (env[[".epiprocess::iterator_started"]]) {
    stop("already started; check that it isn't being used in two places or restarted after a failure")
  } else {
    env[[".epiprocess::iterator_started"]] <- TRUE
  }
}

# TODO rename? input doesn't have to be list
list_itr <- function(x) {
  i <- 0L
  lim <- length(x)
  new_epi_sized_iterator(
    length(x),
    function() {
      if (!`.epiprocess::iterator_started`) {
        stop("start the iterator first")
      }
      if (i == lim) {
        as.symbol(".__exhausted__.")
      } else {
        i <<- i + 1L
        x[[i]]
      }
    }
  )
}

as.list.epi_sized_iterator <- function(itr) {
  itr_start(itr)
  result <- vector("list", attr(itr, "epiprocess::size"))
  for (i in seq_len(attr(itr, "epiprocess::size"))) {
    subresult <- itr()
    if (identical(subresult, as.symbol(".__exhausted__."))) {
      stop("stopped early")
    } else {
      result[[i]] <- subresult
    }
  }
  if (!identical(itr(), as.symbol(".__exhausted__."))) {
    stop("didn't stop in time")
  }
  result
}

# TODO check overhead
itr_res_fmap <- function(itr_res, f) {
  if (identical(itr_res, as.symbol(".__exhausted__."))) {
    as.symbol(".__exhausted__.")
  } else {
    f(itr_res)
  }
}

itr_map_itr <- function(itr, f) {
  itr_start(itr)
  new_epi_sized_iterator(
    itr_size(itr),
    function() itr_res_fmap(itr(), f)
  )
}
