
# request instance/ekrtset, sources -> valset?  but
# what about train test distinctions?

# may need different types of vars based on whether they have already
# selected vlag, tlag, or not

# may want caching layer that performs logic about set of freshly
# requested vs. re-requested instances.  again, training vs. testing
# probably makes annoying

library(dplyr)
library(tidyr)
library(epiprocess)
library(checkmate)

basic_var <- function(source_name, value_name) {
  source_name
  value_name
  function(request_keys, source_list) {
    source_obj <- source_list[[source_name]]
    request_keys %>%
      left_join(source_obj, by = names(request_keys), relationship = "one-to-one")
  }
}

tvlag_var <- function(source_name, value_name, tlag, vlag) {
  source_name
  value_name
  # ^ for simple nowcaster, maybe want to just go with single-archive
  # input data and just take a value_name here; then later, generalize
  # to a lens?
  tlag
  vlag
  function(request_keys, source_list) {
    archive <- source_list[[source_name]]
    assert_class(archive, "epi_archive")
    .........
  }
}

# Should variables have separate functions for training and testing (and evaluation?) fetching?
# - Separate: one path to allowing differing behavior for target
#   variable on train vs test set vs evaluation set (e.g., use
#   semistable if available vs. complain if more stable available
#   vs. use more stable).
#   - but this could also be achieved with target being mapped to
#     different variables in different contexts
# - Separate: one way to allow simulated/predicted/imputed test-time
#   data to be subbed into model trained on real observations (e.g.,
#   in iterative sample models).  Seems would avoid some awkwardness
#   and near-ambiguity/easily-messed-up-notation in conditional
#   notation (if want Y ~ linear_features | partition/weighting-stuff,
#   then Y ~ linear_features | another_feature =
#   another_feature_imputed might imply something else, and actually Y
#   ~ linear_features | another_feature ~
#   another_feature_sample... well no, this has ~ not =, it's fine,
#   and "proper" way of writing, Y ~ linear_features |
#   feature = imputed_feature)
#   - but as weighting stuff would require some sort of function on
#     RHS like Y ~ X | w(var), seems like might be able to require
#     function even for discrete stratification? |
#     stratified(geo_value)? maybe looks bad though
# - Combined: we don't need differing behavior for predictors, so
#   predictors could be characterized more cleanly as one function
# - Combined: it's unclear how many contexts there are... train and
#   test, maybe, but there's also evaluation, and maybe other
#   contexts.  More hassle to write and are we really going to make
#   the exact desired combination of choices desired?
