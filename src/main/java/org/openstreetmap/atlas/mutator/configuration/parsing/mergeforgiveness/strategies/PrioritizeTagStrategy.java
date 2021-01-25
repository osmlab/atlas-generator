package org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.strategies;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.MergeForgivenessStrategy;
import org.openstreetmap.atlas.tags.filters.TaggableFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MergeForgivenessStrategy} which prioritizes the {@link FeatureChange} that matches a
 * {@link TaggableFilter} specified in the configuration, if possible. See docs for the
 * {@link PrioritizeTagStrategy#resolve(FeatureChange, FeatureChange, Map)} method for more details.
 *
 * @author lcram
 */
public class PrioritizeTagStrategy implements MergeForgivenessStrategy
{
    /**
     * A configuration bean class to represent a prioritization rule. Contains fields to store the
     * {@link TaggableFilter} as well as some other configuration variables.
     *
     * @author lcram
     */
    public static class PrioritizationRule
    {
        static final String NAME_CONFIG = "name";
        static final String FILTER_CONFIG = "filter";
        static final String MUTUALLY_EXCLUSIVE_CONFIG = "mutuallyExclusive";
        static final String PAIRWISE_EXCLUSIVE_CONFIG = "pairwiseExclusive";
        static final String LEFT_RIGHT_EXCLUSIVE_CONFIG = "leftRightExclusive";
        static final String ATTITUDE_CONFIG = "attitude";

        private final String name;
        private final TaggableFilter filter;
        private final boolean mutuallyExclusive;
        private final List<String> pairwiseExclusivePartners;
        private final boolean leftRightExclusive;
        private final ConflictResolutionAttitude attitude;

        PrioritizationRule(final String name, final TaggableFilter filter,
                final boolean mutuallyExclusive, final List<String> pairwiseExclusivePartners,
                final boolean leftRightExclusive, final ConflictResolutionAttitude attitude)
        {
            this.name = name;
            this.filter = filter;
            this.mutuallyExclusive = mutuallyExclusive;
            this.pairwiseExclusivePartners = pairwiseExclusivePartners;
            this.leftRightExclusive = leftRightExclusive;
            this.attitude = attitude;
        }

        @Override
        public boolean equals(final Object object)
        {
            if (this == object)
            {
                return true;
            }
            if (object == null || getClass() != object.getClass())
            {
                return false;
            }
            final PrioritizationRule that = (PrioritizationRule) object;
            /*
             * We only need to check against the name since we enforce name unique-ness in the
             * parsing step.
             */
            return Objects.equals(this.name, that.name);
        }

        public ConflictResolutionAttitude getAttitude()
        {
            return this.attitude;
        }

        public TaggableFilter getFilter()
        {
            return this.filter;
        }

        public String getName()
        {
            return this.name;
        }

        public List<String> getPairwiseExclusivePartners()
        {
            return this.pairwiseExclusivePartners;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(this.name);
        }

        public boolean isLeftRightExclusive()
        {
            return this.leftRightExclusive;
        }

        public boolean isMutuallyExclusive()
        {
            return this.mutuallyExclusive;
        }

        @Override
        public String toString()
        {
            return "PrioritizationRule{" + "name='" + this.name + '\'' + ", filter=" + this.filter
                    + ", mutuallyExclusive=" + this.mutuallyExclusive
                    + ", pairwiseExclusivePartners=" + this.pairwiseExclusivePartners
                    + ", leftRightExclusive=" + this.leftRightExclusive + ", attitude="
                    + this.attitude + '}';
        }
    }

    /**
     * The {@link ConflictResolutionAttitude} refers to how {@link PrioritizeTagStrategy} decides to
     * handle cases where more than one prioritization rule may apply. In general, a STRICT
     * application means that if a conditional violation is found (e.g. mutual exclusivity), we
     * fail. A LENIENT application means we may silently cull the offending rule and try another.
     * NOTE: we may end up ditching this concept entirely once I have gathered more data about the
     * nature of these errors.
     *
     * @author lcram
     */
    private enum ConflictResolutionAttitude
    {
        STRICT,
        LENIENT
    }

    static final String RULES = "rules";

    private static final long serialVersionUID = 1587521150202532598L;
    private static final Logger logger = LoggerFactory.getLogger(PrioritizeTagStrategy.class);

    /**
     * Perform the resolution. At the moment, we do a basic resolution that tries to apply as many
     * filters as possible. If the number that apply is not exactly 1, we fail. If both the left and
     * right {@link FeatureChange} apply for a given rule, we decide what to do based on the
     * leftRightExclusivity parameter. In the future, this method may respect the other parameters
     * as well. For now, we will see what the data looks like using this simple resolution.
     *
     * @param left
     *            the left {@link FeatureChange}
     * @param right
     *            the right {@link FeatureChange}
     * @param configuration
     *            the associated configuration {@link Map}
     * @return the selected {@link FeatureChange}
     */
    @Override
    public FeatureChange resolve(final FeatureChange left, final FeatureChange right,
            final Map<String, Serializable> configuration)
    {
        logger.error("Trying PrioritizeTagStrategy on conflict:\n{}\nvs\n{}\nUsing raw config:\n{}",
                left.prettify(), right.prettify(), configuration);
        final List<PrioritizationRule> prioritizationRules = getPrioritizationRules(configuration);
        Map<PrioritizationRule, FeatureChange> ruleToSelectedChanges = computeRulesToSelectedChanges(
                left, right, prioritizationRules);

        if (ruleToSelectedChanges.isEmpty())
        {
            throw new CoreException(
                    "Unable to select a FeatureChange from\n{}\nvs\n{}\nNeither exclusively matched any provided filters:\n{}",
                    left.prettify(), right.prettify(), prioritizationRules);
        }

        if (ruleToSelectedChanges.size() > 1)
        {
            ruleToSelectedChanges = filterSelectionDownToSingleEntry(ruleToSelectedChanges);
        }

        final Map.Entry<PrioritizationRule, FeatureChange> selectedEntry = new ArrayList<>(
                ruleToSelectedChanges.entrySet()).get(0);
        logger.error(
                "Successfully applied PrioritizeTagStrategy to conflict:\n{}\nvs\n{}\nChose:\n{}\nUsing rule:\n{}",
                left.prettify(), right.prettify(), selectedEntry.getValue(),
                selectedEntry.getKey());
        return selectedEntry.getValue();
    }

    private Map<PrioritizationRule, FeatureChange> computeRulesToSelectedChanges(
            final FeatureChange left, final FeatureChange right,
            final List<PrioritizationRule> prioritizationRules)
    {
        final Map<PrioritizationRule, FeatureChange> ruleToSelectedChanges = new HashMap<>();
        for (final PrioritizationRule prioritizationRule : prioritizationRules) // NOSONAR
        {
            final boolean leftTagsMatchFilter = left.getTags() != null
                    && prioritizationRule.getFilter().test(left);
            final boolean rightTagsMatchFilter = right.getTags() != null
                    && prioritizationRule.getFilter().test(right);

            if (leftTagsMatchFilter && rightTagsMatchFilter)
            {
                if (!prioritizationRule.isLeftRightExclusive())
                {
                    ruleToSelectedChanges.put(prioritizationRule, left);
                    break;
                }
                logger.error(
                        "Skipping resolution of:\n{}\nvs\n{}\nWith rule:\n{}\nsince both matched filter and leftRightExclusive=true",
                        left.prettify(), right.prettify(), prioritizationRule);
            }
            else if (leftTagsMatchFilter)
            {
                ruleToSelectedChanges.put(prioritizationRule, left);
                break;
            }
            else if (rightTagsMatchFilter)
            {
                ruleToSelectedChanges.put(prioritizationRule, right);
                break;
            }
        }
        return ruleToSelectedChanges;
    }

    private Map<PrioritizationRule, FeatureChange> filterSelectionDownToSingleEntry(
            final Map<PrioritizationRule, FeatureChange> ruleToSelectedChanges)
    {
        // TODO this should actually do some kind of merge
        throw new CoreException("Unable to select a FeatureChange. Too many matched:\n{}",
                ruleToSelectedChanges);
    }

    @SuppressWarnings("unchecked")
    private List<PrioritizationRule> getPrioritizationRules(
            final Map<String, Serializable> configuration)
    {
        final List<PrioritizationRule> configurations = new ArrayList<>();
        final List<Map<String, Object>> rawConfig = (List<Map<String, Object>>) configuration
                .get(RULES);
        final Set<String> namesSeen = new HashSet<>();

        for (final Map<String, Object> rawMap : rawConfig)
        {
            final String name = (String) rawMap.get(PrioritizationRule.NAME_CONFIG);
            if (name == null)
            {
                throw new CoreException("{} was missing required key \"{}\"",
                        PrioritizationRule.class.getName(), PrioritizationRule.NAME_CONFIG);
            }
            if (namesSeen.contains(name))
            {
                throw new CoreException("Already saw a {} with name \"{}\"",
                        PrioritizationRule.class.getName(), name);
            }
            namesSeen.add(name);

            final String filterString = (String) rawMap.get(PrioritizationRule.FILTER_CONFIG);
            if (filterString == null)
            {
                throw new CoreException("{}:{} was missing required key \"{}\"",
                        PrioritizationRule.class.getName(), name, PrioritizationRule.FILTER_CONFIG);
            }
            final TaggableFilter filter = TaggableFilter.forDefinition(filterString);

            final String precedenceString = (String) rawMap.getOrDefault(
                    PrioritizationRule.ATTITUDE_CONFIG,
                    ConflictResolutionAttitude.STRICT.toString());
            final ConflictResolutionAttitude precedence = ConflictResolutionAttitude
                    .valueOf(precedenceString);

            final boolean mutuallyExclusive = (boolean) rawMap
                    .getOrDefault(PrioritizationRule.MUTUALLY_EXCLUSIVE_CONFIG, true);
            final List<String> pairwiseExclusive = (List<String>) rawMap
                    .getOrDefault(PrioritizationRule.PAIRWISE_EXCLUSIVE_CONFIG, new ArrayList<>());
            final boolean leftRightExclusive = (boolean) rawMap
                    .getOrDefault(PrioritizationRule.LEFT_RIGHT_EXCLUSIVE_CONFIG, true);

            configurations.add(new PrioritizationRule(name, filter, mutuallyExclusive,
                    pairwiseExclusive, leftRightExclusive, precedence));
        }

        return configurations;
    }
}
