





library(coro)

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

snap_iterator_factory4 <- function(subtbls) {
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

bench::mark(
{
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
{
  snaps2 <- collect(snap_generator_factory(grp_updates$subtbl))
},
{
  snaps3 <- vector("list", length(grp_updates$subtbl))
  snap_generator_alt <- snap_generator_factory_alt(grp_updates$subtbl)
  for (i in seq_len(nrow(grp_updates))) {
    snaps3[[i]] <- snap_generator_alt()
  }
  snaps3
},
{
  snap_iterator4 <- snap_iterator_factory4(grp_updates$subtbl)
  collect(snap_iterator4)
},
{
  snap_iterator4 <- snap_iterator_factory4(grp_updates$subtbl)
  results <- list()
  while(TRUE) {
    result <- snap_iterator4() # we already know there's no `close` arg to provide.
    if (coro::is_exhausted(result)) {
      break
    } else {
      results <- c(results, list(result))
    }
  }
  results
},
min_time = 3
)
#> # A tibble: 5 × 13
#>   expression      min median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result memory     time       gc      
#>   <bch:expr>    <bch> <bch:>     <dbl> <bch:byt>    <dbl> <int> <dbl>   <bch:tm> <list> <list>     <list>     <list>  
#> 1 "{ prev_snap… 214ms  216ms      4.61    22.4MB     3.46     8     6      1.73s <list> <Rprofmem> <bench_tm> <tibble>
#> 2 "{ snaps2 <-… 378ms  379ms      2.64    22.8MB     7.93     2     6   757.01ms <list> <Rprofmem> <bench_tm> <tibble>
#> 3 "{ snaps3 <-… 214ms  216ms      4.63    22.4MB     3.47     8     6      1.73s <list> <Rprofmem> <bench_tm> <tibble>
#> 4 "{ snap_iter… 236ms  239ms      4.17    22.4MB     3.57     7     6      1.68s <list> <Rprofmem> <bench_tm> <tibble>
#> 5 "{ snap_iter… 221ms  225ms      4.42    23.6MB     2.76     8     5      1.81s <list> <Rprofmem> <bench_tm> <tibble>

# coro generators seem to have a lot of overhead.  And coro::collect() overhead is notable.  Though if we were doing the inverse operation taking snapshots and forming an archive, and those snapshots were on disk, then maybe we would benefit from coro async generators.

as_iterator(1:3)
i <- as_iterator(1:3)
loop(for (x in i) print(x))

# XXX is coro overhead just from generator() & yield()?  is it possible to make a coro-compatible iterator along the line of snaps3 approach?
#
# --- seems possible; see ?exhausted

# TODO also check iterators, itertools, itertools2, foreach packages
