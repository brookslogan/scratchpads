


# TODO try different max delay and training window parms, then also with aux covariates

withr::with_rng_version("3.5", withr::with_seed(4120329, {
  # n <- 100L
  # n <- 200L
  # n <- 50L
  # n <- 500L
  # n <- 1000L
  # n <- 10000L
  n <- 30L
  # n <- 50L
  # n <- 10L
  # n <- 15L
  # n <- 20L
  # n <- 10000L
  # TODO replicates
  sapply(1:1000, function(sim_i) {
    trend <- plogis(5*sin(1:n/52*2*pi) - 5) %>%
      # {. / mean(.) * 200e3 / 52 * 0.001}
      {. / mean(.) * 200e3 / 52 * 0.01}
    finalized <- rpois(n, trend)
    delay_dist <- (5:1) %>% {. / sum(.)}
    # delay_dist <- c(8, .5, .5, .5, .5) %>% {. / sum(.)}
    ar_feats <- t(sapply(finalized, function(size) rmultinom(1L, size, delay_dist))) %>%
      {sapply(1:ncol(.), function(j) dplyr::lag(.[,j], j - 1L))} %>%
      {.[is.na(.)] <- 0L; .}
    lm1 <- lm(finalized ~ ar_feats)
    lm2 <- lm(finalized ~ ar_feats + 0)
    # trend2 <- trend
    trend2 <- plogis(5*cos((1:n)/52*2*pi) - 5) %>%
      # {. / mean(.) * 200e3 / 52 * 0.001}
      {. / mean(.) * 200e3 / 52 * 0.01}
    finalized2 <- rpois(n, trend2)
    ar_feats2 <- t(sapply(finalized2, function(size) rmultinom(1L, size, delay_dist))) %>%
      {sapply(1:ncol(.), function(j) dplyr::lag(.[,j], j - 1L))} %>%
      {.[is.na(.)] <- 0L; .}
    pred1 <- predict(lm1, newdata = tibble(ar_feats = ar_feats2))
    pred2 <- predict(lm2, newdata = tibble(ar_feats = ar_feats2))
    c(
      mean(abs(ar_feats2[1,] - finalized2)) / mean(abs(finalized2 - dplyr::lag(finalized2)), na.rm = TRUE),
      mean(abs(pred1 - finalized2)) / mean(abs(finalized2 - dplyr::lag(finalized2)), na.rm = TRUE),
      mean(abs(pred2 - finalized2)) / mean(abs(finalized2 - dplyr::lag(finalized2)), na.rm = TRUE),
      mean(abs(trend2 - finalized2)) / mean(abs(finalized2 - dplyr::lag(finalized2)), na.rm = TRUE)
    )
  })
})) %>%
  rowMeans()

# Small training set sizes don't seem too bad.  Surprising but not
# entirely since more rigid approaches worked on weekly data with
# "small" windows.






# think about collinearity detection

N <- 100L
y <- rnorm(N)
x1 <- rep(0, N)
x2 <- rep(0, N)

lm(y ~ x1 + x2)

quantreg::rq(y ~ x1 + x2)

quantreg::rq(y ~ x1 + x2, method = "fn")

# maybe don't try as build feature by feature.  just copy over handling from epiforecast-R






# Dealing with missingness... Sequentially going through list and
# having an allowed new-missingness proportion and overall missingness
# proportion?  Greedy algorithm based on priority levels and
# missingness (how to combine?)?  Existing advice on variable subset
# selection in the presence of missingness and somewhat low sample
# size?  Probably assuming MCAR (prob okay if due to lag / sporadic
# outages, maybe not if censoring...), linear Gaussian (...), ...

# https://theory.stanford.edu/~jvondrak/data/submod-tutorial-1.pdf

# submodular (monotonic) min might apply to finding "min missingness
# cover", though desired weighting for covariate importance and
# constraints (e.g., on cardinality) might make difficult again.
# Might think about geometric programming (inspired?) approach.
# Plus general-purpose algorithms are complicated...

# With weightings... rather than set function on cols, max priority
# level 2-D submatrix sum(?) on matrix w/ -Inf for missingness that
# naturally selects all rows w/o missingness given colset but might
# have different, helpful/harmful structure? Not nec. contiguous ->
# not Kadane; GP? no, want mixed sign... except is sum actually the
# right combiner?;
# https://yetanothermathprogrammingconsultant.blogspot.com/2021/01/submatrix-with-largest-sum.html



# https://web.stanford.edu/~rjohari/teaching/notes/226_lecture5_prediction.pdf

# true_mean <- rexp(5)
# true_covar <- runif(5*5) %>% matrix(5,5) %>% {. %*% t(.)}

# sample1 <- MASS::mvrnorm(10000L, true_mean, true_covar)
# x1 <- sample1[, 1:4, drop = FALSE]
# y1 <- sample1[,   5, drop = TRUE]
# lm(y1 ~ x1) %>% .$residuals %>% {mean(.^2)}

# sample2 <- MASS::mvrnorm(10L, true_mean, true_covar)

# x2 <- sample2[, 1:4, drop = FALSE]
# x2 <- sample2[,   1, drop = FALSE]
# x2 <- sample2[,   2, drop = FALSE]
# x2 <- sample2[,   3, drop = FALSE]
# x2 <- sample2[,   4, drop = FALSE]
# x2 <- sample2[, 1:2, drop = FALSE]
# x2 <- sample2[, 2:3, drop = FALSE]
# x2 <- sample2[, 3:4, drop = FALSE]
# y2 <- sample2[,   5, drop = TRUE]
# lm(y2 ~ x2) %>% .$residuals %>% {mean(.^2)} # just one instance, not expectation, but still not sure how this is supposed to be affine in p... oh, probably means fixed p & covar, variable n
# lm(y2 ~ x2) %>% predict(newx = x1) %>% {y1 - .} %>% {mean(.^2)}

# https://david-kempe.com/publications/regression.pdf

# https://arxiv.org/pdf/1510.06301

# https://www.math.princeton.edu/events/new-method-best-subset-selection-problem-2019-12-02t210000
# ... is this the general definition of the NP-hard problem?  while
# max n incorporated features does seem similar with sparsity
# constraint, a version just based on expected prediction error
# factoring in overfitting seems potentially different, and would
# already be serving a similar purpose...

# https://arxiv.org/html/2511.02740v1
