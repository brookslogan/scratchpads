
new_epi_sized_iteratorb <- function(size, f) {
  assert_int(size, lower = 0)
  assert_function(f, args = "i")
  assert_false(inherits(f, "epi_sized_iteratorb"))

  itrb <- f
  attr(itrb, "epiprocess::size") <- size
  class(itrb) <- "epi_sized_iteratorb"
  itrb
}

# XXX or for debuggability should iterators be indexing into something but not have to keep around previous elements?  to prevent accidental advances.  even with start/claim there are limits

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
  curr_i <- 0L
  curr_result <- NULL
  new_epi_sized_iteratorb(
    itrb_size(itrb),
    function(i) {
      if (i == curr_i) {
        if (curr_i == 0L) {
          stop("0 out of bounds")
        } else {
          curr_result
        }
      } else {
        result <- f(itrb(i))
        # FIXME atomicity
        curr_i <<- i
        curr_result <<- result
        curr_result
      }
    }
  )
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
