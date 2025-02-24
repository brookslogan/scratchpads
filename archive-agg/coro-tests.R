





library(coro)
library(iterators)
library(itertools2)

snap_generator_factory <- generator(function(subtbls) {
  prev_snap <- NULL
  for (update in subtbls) {
    update <- as_tibble(as.data.frame(update))
    if (is.null(prev_snap)) {
      snap <- update
    } else {
      snap <- tbl_patch(prev_snap, update, "time_value")
    }
    yield(snap)
    prev_snap <- snap
  }
})

snap_generator_factory_alt <- function(subtbls) {
  prev_snap <- NULL
  i <- 1
  function() {
    if (i == length(subtbls) + 1) {
      ## NULL
      stop("should have limited generation to within the limit")
    } else {
      update <- subtbls[[i]]
      update <- as_tibble(as.data.frame(update))
      if (is.null(prev_snap)) {
        snap <- update
      } else {
        snap <- tbl_patch(prev_snap, update, "time_value")
      }
      prev_snap <<- snap
      i <<- i + 1
      snap
    }
  }
}
# TODO change to make function take in the next update and have downstream
# perform all iteration ... but does this just mean that the right abstraction
# is a (stateful) mapper/"streamer"? though stateful mapper/streamer might more
# heavily suggest that we don't know/fix the size, which might make
# pre-allocation and progress look more weird/annoying than augmenting an
# iterator with the size to be more like a range. Size-augmented iterator /
# range might be the right concept, but might not exist in a pre-existing
# library...

snap_iterator_factory <- function(subtbls) {
  prev_snap <- NULL
  i <- 1
  function() {
    if (i == length(subtbls) + 1) {
      as.symbol(".__exhausted__.")
    } else {
      update <- subtbls[[i]]
      update <- as_tibble(as.data.frame(update))
      if (is.null(prev_snap)) {
        snap <- update
      } else {
        snap <- tbl_patch(prev_snap, update, "time_value")
      }
      prev_snap <<- snap
      i <<- i + 1
      snap
    }
  }
}

snap_iteratorbf_factory <- function(subtbls) {
  curr_snap <- NULL
  curr_i <- 0L
  function(i) {
    if (i == curr_i + 1L) { # perf: common case first
      if (curr_i == length(subtbls)) {
        stop("requested beyond size")
      }
      update <- subtbls[[i]]
      update <- as_tibble(as.data.frame(update))
      if (is.null(curr_snap)) {
        snap <- update
      } else {
        snap <- tbl_patch(curr_snap, update, "time_value")
      }
      curr_i <<- i
      curr_snap <<- snap
      curr_snap
    } else if (i == curr_i) {
      curr_snap
    } else {
      stop("needs to be current or next i")
    }
  }
}

snap_iter_factory <- function(subtbls) {
  # ... would be nice to just use iter(subtbls) %>% iter_map_iter sort of thing,
  # but itertools2::imap doesn't seem to be it, can't find anything like this?
  # Also don't know if function calls involved would actually be significant
  # overhead or not...
  prev_snap <- NULL
  i <- 1
  iter <- list(
    state = environment(), # is this okay ???
    length = length(subtbls),
    # checkFunc???
    hasNext = function() {
      i != length(subtbls) + 1
    },
    nextElem = function() {
      if (i == length(subtbls) + 1) {
        stop("StopIteration")
      } else {
        update <- subtbls[[i]]
        update <- as_tibble(as.data.frame(update))
        if (is.null(prev_snap)) {
          snap <- update
        } else {
          snap <- tbl_patch(prev_snap, update, "time_value")
        }
        prev_snap <<- snap
        i <<- i + 1
        snap
      }
    }
  ) %>% `class<-`(c("abstractiter", "iter")) # ???
}

bench::mark(
map = {
  prev_snap <- NULL
  snaps1 <- map(grp_updates$subtbl, function(update) {
    update <- as_tibble(as.data.frame(update))
    snap <-
      if (is.null(prev_snap)) {
        update
      } else {
        tbl_patch(prev_snap, update, "time_value")
      }
    prev_snap <<- snap
    snap
  })
},
## collect_generator = {
##   snaps2 <- collect(snap_generator_factory(grp_updates$subtbl))
## },
manual_collect_sized_iterator = {
  snaps3 <- vector("list", length(grp_updates$subtbl))
  snap_generator_alt <- snap_generator_factory_alt(grp_updates$subtbl)
  for (i in seq_len(nrow(grp_updates))) {
    snaps3[[i]] <- snap_generator_alt()
  }
  snaps3
},
## collect_manual_iterator = {
##   snap_iterator4 <- snap_iterator_factory(grp_updates$subtbl)
##   collect(snap_iterator4)
## },
## manual_collect_unsized_iterator = {
##   snap_iterator5 <- snap_iterator_factory(grp_updates$subtbl)
##   results <- list()
##   while(TRUE) {
##     result <- snap_iterator5() # we already know there's no `close` arg to provide.
##     if (coro::is_exhausted(result)) {
##       break
##     } else {
##       results <- c(results, list(result))
##     }
##   }
##   results
## },
## iter = {
##   snap_iter6 <- snap_iter_factory(grp_updates$subtbl)
##   as.list(snap_iter6)
## },
szitrclass = {
  szitr <- new_epi_sized_iterator(nrow(grp_updates), snap_iterator_factory(grp_updates$subtbl))
  as.list(szitr)
},
szitrclassb = {
  szitrb <- new_epi_sized_iteratorb(nrow(grp_updates), snap_iteratorbf_factory(grp_updates$subtbl))
  as.list(szitrb)
},
min_time = 60
)

# coro generators seem to have a lot of overhead.  And coro::collect() overhead is notable.  Though if we were doing the inverse operation taking snapshots and forming an archive, and those snapshots were on disk, then maybe we would benefit from coro async generators.

as_iterator(1:3)
i <- as_iterator(1:3)
loop(for (x in i) print(x))

# XXX is coro overhead just from generator() & yield()?  is it possible to make a coro-compatible iterator along the line of snaps3 approach?
#
# --- seems possible; see ?exhausted

# TODO also check iterators, itertools, itertools2, foreach packages

# kind of lost trying to build on iterators package...
