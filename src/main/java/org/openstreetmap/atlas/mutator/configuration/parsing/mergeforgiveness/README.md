# Merge Forgiveness

The `atlas-mutator` supports the ability to forgive `FeatureChangeMergeException`s in specific, configurable cases. The `mergeForgivenessPolicy`
can be specified at the `global` level, wherein the platform will forgive specific `FeatureChangeMergeException`s that occur between mutations
at a the same level and country. The `mergeForgivenessPolicy` can also be specified at a mutation-local level, wherein the platform will forgive
specific `FeatureChangeMergeException`s that occur within a mutation.

## Learning by example - a global configuration
First, let's take a peek at what a typical merge failure looks like:
```
Caused by: org.openstreetmap.atlas.exception.change.FeatureChangeMergeException: Cannot merge two feature changes:
FeatureChange [
changeType: ADD, 
itemType: AREA, 
identifier: 1, 
bfView: CompleteArea [identifier: 1, polygon: POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0)), ], 
afView: CompleteArea [identifier: 1, polygon: POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0)), ], 
metadata: {mutator=FooMutation|8-148-127}
]
AND
FeatureChange [
changeType: ADD, 
itemType: AREA, 
identifier: 1, 
bfView: CompleteArea [identifier: 1, polygon: POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0)), ], 
afView: CompleteArea [identifier: 1, polygon: POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0)), ], 
metadata: {mutator=BarMutation|8-148-127}
]
FailureTrace: [MUTUALLY_EXCLUSIVE_ADD_ADD_CONFLICT, DIFF_BASED_POLYGON_MERGE_FAIL, AFTER_VIEW_CONSISTENT_BEFORE_VIEW_MERGE_STRATEGY_FAILED, HIGHEST_LEVEL_MERGE_FAILURE]
Caused by: org.openstreetmap.atlas.exception.change.FeatureChangeMergeException: Attempted afterViewConsistentBeforeMerge failed for polygon with beforeView:
POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))
afterView:
POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))
vs
POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))
    at org.openstreetmap.atlas.geography.atlas.change.MemberMerger.mergeMembersWithConsistentBeforeViews(MemberMerger.java:575)
    at org.openstreetmap.atlas.geography.atlas.change.MemberMerger.mergeMember(MemberMerger.java:283)
    at org.openstreetmap.atlas.geography.atlas.change.FeatureChangeMergingHelpers.mergeAreas(FeatureChangeMergingHelpers.java:287)
    at org.openstreetmap.atlas.geography.atlas.change.FeatureChangeMergingHelpers.mergeADDFeatureChangePair(FeatureChangeMergingHelpers.java:112)
    at org.openstreetmap.atlas.geography.atlas.change.FeatureChange.merge(FeatureChange.java:482)
    ... 29 more
Caused by: org.openstreetmap.atlas.exception.change.FeatureChangeMergeException: mutually exclusive Polygon merge failed
    at org.openstreetmap.atlas.geography.atlas.change.MemberMergeStrategies.lambda$static$21(MemberMergeStrategies.java:258)
    at org.openstreetmap.atlas.geography.atlas.change.MemberMerger.mergeMembersWithConsistentBeforeViews(MemberMerger.java:571)
    ... 33 more
Caused by: org.openstreetmap.atlas.exception.change.FeatureChangeMergeException: diffBasedMutuallyExclusiveMerger failed due to ADD/ADD conflict: beforeView was:
POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))
but afterViews were:
POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))
vs
POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))
    at org.openstreetmap.atlas.geography.atlas.change.MemberMergeStrategies.lambda$getDiffBasedMutuallyExclusiveMerger$37(MemberMergeStrategies.java:787)
    at org.openstreetmap.atlas.geography.atlas.change.MemberMergeStrategies.lambda$static$21(MemberMergeStrategies.java:251)
    ... 34 more
```
Let's break down this trace. The top part is showing us a full textual representation of the conflicting `FeatureChange`s. We see that they are both
`ADD`s, and they are operating on an `Area` with ID 1. Note the `metadata` field, which shows us the mutation and shard from which the change
originated. If the metadata is empty, that indicates the `FeatureChange`s came from the same mutation.  This can happen in cases where a mutation
generates conflicts with itself (oops! more on this later).

Next, note the line starting with `FailureTrace:`. This line gives us a complete summary of the chain of failures that caused the merge conflict -
where reading left-to-right reads from most specific to most general. If you want more details on what these failures mean, check the
[`MergeFailureType`](https://github.com/osmlab/atlas/blob/dev/src/main/java/org/openstreetmap/atlas/exception/change/MergeFailureType.java) enum in `atlas` where each is defined.

Finally, the rest of the stack trace is showing us details about the specific member caused the conflict - in this case it's the polygon.
Here we see that the `FeatureChange` merge code attempted to use an afterView merger that requires a consistent beforeView. This makes
sense, since we have conflicting afterViews but harmonious beforeViews (There are some cases, like for Relation member lists, where we may
even allow conflicting beforeViews due to sharding artifacts). At the very bottom of the stack trace we see the root level failure, that the
`diffBasedMutuallyExclusiveMerger` failed due to an ADD/ADD conflict (for more details on the `diffBasedMutuallyExclusiveMerger`, see
[`MemberMergeStrategies#getDiffBasedMutuallyExclusiveMerger`](https://github.com/osmlab/atlas/blob/0dc4b360c99ba16a65a8560b627ef7e0dd048e47/src/main/java/org/openstreetmap/atlas/geography/atlas/change/MemberMergeStrategies.java#L750) in `atlas`).

Now suppose we want to forgive this type of failure, and we want the policy to always choose the left side `FeatureChange` (in other words, pick one
arbitrarily). In that case, we can specify a global configuration like:
```
{
    "global":
    {
        "mergeForgivenessPolicy":
        {
            "resolvableExactSequenceFailures":
            [
                {
                    "exactSequenceFailure": ["MUTUALLY_EXCLUSIVE_ADD_ADD_CONFLICT", "DIFF_BASED_POLYGON_MERGE_FAIL", "AFTER_VIEW_CONSISTENT_BEFORE_VIEW_MERGE_STRATEGY_FAILED", "HIGHEST_LEVEL_MERGE_FAILURE"],
                    "strategyClassName": "org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.strategies.AlwaysPickLeftStrategy"
                }
            ]
        }
    }
}
```
Here, we specified an exact failure to sequence to catch. Notice that the field `exactFailureSequence` exactly matches the sequence given in the
stack trace on the line starting with `FailureTrace:`. The strategy will not be applied to a `FailureTrace:` that does not **exactly** match
this sequence.

Now suppose we want to handle any `MUTUALLY_EXCLUSIVE_ADD_ADD_CONFLICT`, regardless of if it occurred for polygons, polylines, locations, or whatever.
In that case, we could use `resolvableRootLevelFailures` instead, which will attempt to match against just the root level (leftmost) failure
in the trace:
```
{
    "global":
    {
        "mergeForgivenessPolicy":
        {
            "resolvableRootLevelFailures":
            [
                {
                    "rootLevelFailure": "MUTUALLY_EXCLUSIVE_ADD_ADD_CONFLICT",
                    "strategyClassName": "org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.strategies.AlwaysPickLeftStrategy"
                }
            ]
        }
    }
}
```

Note that you can also mix and match! The strategy application will attempt to apply the most specific strategy first. So perhaps you want
to resolve polygon `ADD/ADD` conflicts by picking the larger polygon, but you want to resolve any other `ADD/ADD` by just choosing the left side
In that case, try:
```
{
    "global":
    {
        "mergeForgivenessPolicy":
        {
            "resolvableExactSequenceFailures":
            [
                {
                    "exactSequenceFailure": ["MUTUALLY_EXCLUSIVE_ADD_ADD_CONFLICT", "DIFF_BASED_POLYGON_MERGE_FAIL", "AFTER_VIEW_CONSISTENT_BEFORE_VIEW_MERGE_STRATEGY_FAILED", "HIGHEST_LEVEL_MERGE_FAILURE"],
                    "strategyClassName": "some.madeup.package.PickLargerPolygonStrategy"
                    "strategyConfiguration":
                    {
                        "foo": "bar"
                    }
                }
            ],
            "resolvableRootLevelFailures":
            [
                {
                    "rootLevelFailure": "MUTUALLY_EXCLUSIVE_ADD_ADD_CONFLICT",
                    "strategyClassName": "org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.strategies.AlwaysPickLeftStrategy"
                }
            ]
        }
    }
}
```
Notice that for the `PickLargerPolygonStrategy`, we provided a strategy configuration. This is a feature that some strategies may choose to support.

## Handling an inner mutation conflict
Occasionally a mutation will generate self-conflicting `FeatureChange`s. In this case, we can configure a merge forgiveness policy for specific
mutations. Suppose we have a mutation `FooMutation` that is generating `ADD` `FeatureChange`s updating a feature, but then it decides to
`REMOVE` the feature all together. We can forgive this issue with the following configuration:
```
{
    "global":
    {
        "scanUrls": ["org.openstreetmap.atlas"],
        "validate": true
    },
    "FooMutation":
    {
        "className": "FooMutation",
        "enabled": true,
        "mergeForgivenessPolicy":
        {
            "resolvableExactSequenceFailures":
            [
                {
                    "exactSequenceFailure": ["FEATURE_CHANGE_INVALID_ADD_REMOVE_MERGE", "HIGHEST_LEVEL_MERGE_FAILURE"],
                    "strategyClassName": "org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.strategies.PickRemoveOverAddStrategy",
                }
            ]
        },
        "countries": ["FOO"]
    },
}
```
