
# request instance/ekrtset, sources -> valset?  but
# what about train test distinctions?

# XXX have gone from ekv -> val to ekrt -> val, but really might need
# to be ekrt -> val, as there is an implicit nowcast date version in
# things that seem like ekrt -> val; e.g., latest, semistable, ...;
# neglecting and implicit might produce issues with generality
# (tvoffset) and composability; implicit might produce issues with
# joining with non-version-aware structures (maybe good, maybe quite
# inconvenient); explicit might be wordy.  Or maybe have been
# forgetting the "latest" part in notation which is what turns the
# version to "implicit" even though it's not really. Confkey lookup
# also means "latest" isn't necessarily the nowcast date; need to be
# wary of assumptions. Though confkey + tvoffset may actually be not
# quite the same issue... tvoffset also yields an archive
# conceptually, and has an implicit nowcast date attached... but
# that's still better treatable as just one version tag, not two?;
# tvoffset on ekt can be just ekt+value, and then version is just
# nowcast date.  Confkey attached version can hopefully be limited to
# treatment as a data-informed/fit parameter plus an intermediate
# index, not an index attached to the output.
#
# maybe reference time should be more explicitly thought of as a
# parameter derived from the nowcast date / data?
#
# implicit vs. explicit / the nature of the latest() operation impact
# how tvindexing works... relative versions may not work since version
# is implicit; may be forced to represent as vlag from the
# (already-tlagged/toffsetted-from-reference-time) observation
# time_value.
# - no it doesn't: we can calculate offset from reference time as
#   easily as from tlagged reference time, right?

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
    # ^ again, source_list -> archive for simple nowcaster to start?
    archive <- source_list[[source_name]]
    assert_class(archive, "epi_archive")
    stop(".........")
  }
}

# not sure about specifying version - shifted_time_value vs. version -
# reference_time.  And seems hard to name in way that distinguishes,
# plus makes clear that version is relative to a time, not some
# version.  Think this is a ekt -> vals type var, so think don't want
# relative to some version; nowcast_version - testing_reference_time
# will be baked into the offset parameters, rather than being carried
# around in ektv sets.

# XXX in nowcasting helpers branch, may have figured out way to
# enforce forcing of factory args, nice class structure, etc., for
# pipeline segments, which might also apply to var factories.

# XXX special column naming vs. role tracking...

reltv_var <- function(indicator_name, time_rel_rtv, version_rel_rtv) {
  indicator_name
  time_rel_rtv
  version_rel_rtv
  function(request_keys, source_data) {
    assert_class(request_keys, "tbl_df")
    assert_class(source_data, "epi_archive")
    assert_names(names(request_keys), permutation.of = key_colnames(source_data, exclude = "version"))
    #
    request_keys$version <- request_keys$time_value + time_rel_rtv # define
    request_keys$time_value <- request_keys$time_value + time_rel_rtv # shift
    stop("missing vtol")
    stop("...... impl is just extract2_tvoffset? adjust names to match one way or the other?")
    stop("issues getting default vtol... likely want vtol consistent between train and test, but in current design do not have access to train.")
    # - Do we need to construct the variable with the training set
    #   available?  Allows setting vtol right there, as well as name
    #   validation, although still need name validation for the test
    #   set...
    #   - Would this not work when making CV pipeline?
    # - Do we need to have a step/segment to fit/select/train the
    #   vtol? But how would we access that parameter?
    #   - Well, for non-pipeline-framework initial draft, can just
    #     externally calculate on training set and feed into
    #     construction... and that was the plan anyway, for this to be
    #     selected with some "latest" helper, and for this to be able
    #     to work with die cut sums, which for "latest" would use test
    #     proxy from train.  And for pipeline, will have some way to
    #     access parameters... but seems messy.  And this may blow up
    #     the "variables simpler/cleaner than segments" idea.
    #
    # Also, what about delta between "last two" versions?  how does
    # vtol approach work / break here?  do we just need pre-processing
    # to a strict no-tol format?
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
#   - perhaps better counter is that stratification/weighting could be
#     done with steps/factories, also avoiding formula issues
# - Combined: we don't need differing behavior for predictors, so
#   predictors could be characterized more cleanly as one function
# - Combined: it's unclear how many contexts there are... train and
#   test, maybe, but there's also evaluation, and maybe other
#   contexts.  More hassle to write and are we really going to make
#   the exact desired combination of choices desired?
