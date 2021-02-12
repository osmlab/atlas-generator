package org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.exception.change.FeatureChangeMergeException;
import org.openstreetmap.atlas.exception.change.MergeFailureType;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.mutator.configuration.parsing.AtlasMutatorConfigurationParser;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * @author lcram
 */
public class ConfiguredMergeForgivenessPolicy implements Serializable
{
    /**
     * @author lcram
     */
    public static class FailureSequencePolicyElement implements Serializable
    {
        private static final long serialVersionUID = -2707475460873950048L;

        private final List<MergeFailureType> failureSubSequence;
        private final MergeForgivenessStrategy strategy;
        private final Map<String, Serializable> configuration;
        private final boolean exact;

        public FailureSequencePolicyElement(final List<MergeFailureType> failureSubSequence,
                final MergeForgivenessStrategy strategy,
                final Map<String, Serializable> configuration, final boolean exact)
        {
            this.failureSubSequence = failureSubSequence;
            this.strategy = strategy;
            this.configuration = configuration;
            this.exact = exact;
        }

        @Override
        public boolean equals(final Object other)
        {
            if (this == other)
            {
                return true;
            }
            if (other == null || getClass() != other.getClass())
            {
                return false;
            }
            final FailureSequencePolicyElement that = (FailureSequencePolicyElement) other;
            return Objects.equals(this.failureSubSequence, that.failureSubSequence)
                    && Objects.equals(this.strategy, that.strategy)
                    && Objects.equals(this.exact, that.exact);
        }

        public Map<String, Serializable> getConfiguration()
        {
            return this.configuration;
        }

        public List<MergeFailureType> getFailureSequence()
        {
            return this.failureSubSequence;
        }

        public MergeForgivenessStrategy getStrategy()
        {
            return this.strategy;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(this.failureSubSequence, this.strategy);
        }

        public boolean isExact()
        {
            return this.exact;
        }

        @Override
        public String toString()
        {
            return "FailureSequencePolicyElement{" + "failureSubSequence=" + this.failureSubSequence
                    + ", strategy=" + this.strategy.getClass().getName() + ", config="
                    + this.configuration + ", exact=" + this.exact + "}";
        }
    }

    /**
     * @author lcram
     */
    public static class RootLevelPolicyElement implements Serializable
    {
        private static final long serialVersionUID = -3064840935245823460L;

        private final MergeFailureType rootLevelFailureType;
        private final MergeForgivenessStrategy strategy;
        private final Map<String, Serializable> configuration;

        public RootLevelPolicyElement(final MergeFailureType rootLevelFailureType,
                final MergeForgivenessStrategy strategy,
                final Map<String, Serializable> configuration)
        {
            this.rootLevelFailureType = rootLevelFailureType;
            this.strategy = strategy;
            this.configuration = configuration;
        }

        @Override
        public boolean equals(final Object other)
        {
            if (this == other)
            {
                return true;
            }
            if (other == null || getClass() != other.getClass())
            {
                return false;
            }
            final RootLevelPolicyElement that = (RootLevelPolicyElement) other;
            return this.rootLevelFailureType == that.rootLevelFailureType
                    && Objects.equals(this.strategy, that.strategy);
        }

        public Map<String, Serializable> getConfiguration()
        {
            return this.configuration;
        }

        public MergeFailureType getRootLevelFailureType()
        {
            return this.rootLevelFailureType;
        }

        public MergeForgivenessStrategy getStrategy()
        {
            return this.strategy;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(this.rootLevelFailureType, this.strategy);
        }

        @Override
        public String toString()
        {
            return "RootLevelPolicyElement{" + "rootLevelFailureType=" + this.rootLevelFailureType
                    + ", strategy=" + this.strategy.getClass().getName() + ", config="
                    + this.configuration + '}';
        }
    }

    public static final ConfiguredMergeForgivenessPolicy DEFAULT = new ConfiguredMergeForgivenessPolicy();
    public static final String DEFAULT_NAME = "default";
    public static final String CONFIGURATION_ROOT = "mergeForgivenessPolicy";

    public static final String TYPE_JSON_PROPERTY_VALUE = "_mergeForgivenessPolicy";
    public static final String OWNER_JSON_PROPERTY = "owner";
    public static final String ROOT_LEVEL_POLICY_JSON_PROPERTY = "rootLevelPolicy";
    public static final String SUB_SEQUENCE_FAILURE_POLICY_JSON_PROPERTY = "subSequenceFailurePolicy";
    public static final String EXACT_SEQUENCE_FAILURE_POLICY_JSON_PROPERTY = "exactSequenceFailurePolicy";

    private static final long serialVersionUID = 8158886185907696393L;
    private static final Logger logger = LoggerFactory
            .getLogger(ConfiguredMergeForgivenessPolicy.class);
    private static final String GLOBAL_CONFIGURATION_ROOT = AtlasMutatorConfigurationParser.CONFIGURATION_GLOBAL
            + "." + CONFIGURATION_ROOT;
    private static final String CONFIGURATION_RESOLVABLE_ROOT_LEVEL_FAILURES = "resolvableRootLevelFailures";
    private static final String CONFIGURATION_RESOLVABLE_FAILURE_SUBSEQUENCES = "resolvableSubSequenceFailures";
    private static final String CONFIGURATION_RESOLVABLE_FAILURE_EXACT_SEQUENCES = "resolvableExactSequenceFailures";
    private static final String CONFIGURATION_ROOT_LEVEL_FAILURE = "rootLevelFailure";
    private static final String CONFIGURATION_FAILURE_SUBSEQUENCE = "subSequenceFailure";
    private static final String CONFIGURATION_FAILURE_EXACT_SEQUENCE = "exactSequenceFailure";
    private static final String CONFIGURATION_STRATEGY_CLASSNAME = "strategyClassName";
    private static final String CONFIGURATION_STRATEGY_CONFIGURATION = "strategyConfiguration";

    private final String owner;
    private final List<RootLevelPolicyElement> rootLevelFailurePolicy;
    private final List<FailureSequencePolicyElement> subSequenceFailurePolicy;
    private final List<FailureSequencePolicyElement> exactSequenceFailurePolicy;

    public static ConfiguredMergeForgivenessPolicy fromGlobal(final Configuration configuration)
    {
        return new ConfiguredMergeForgivenessPolicy(configuration, GLOBAL_CONFIGURATION_ROOT,
                AtlasMutatorConfigurationParser.CONFIGURATION_GLOBAL);
    }

    public static ConfiguredMergeForgivenessPolicy fromRoot(final Configuration configuration,
            final String root)
    {
        return new ConfiguredMergeForgivenessPolicy(configuration, root + "." + CONFIGURATION_ROOT,
                root);
    }

    private static Optional<MergeForgivenessStrategy> instantiateStrategy(final String classname)
    {
        final Class<?> subcommandClass;
        try
        {
            subcommandClass = Class.forName(classname);
        }
        catch (final ClassNotFoundException exception)
        {
            throw new CoreException("Class {} was not found", classname, exception);
        }

        if (Modifier.isAbstract(subcommandClass.getModifiers()))
        {
            return Optional.empty();
        }

        final Constructor<?> constructor;
        try
        {
            constructor = subcommandClass.getConstructor();
        }
        catch (final NoSuchMethodException exception)
        {
            throw new CoreException("Class {} does not have a matching constructor", classname,
                    exception);
        }
        catch (final SecurityException exception)
        {
            throw new CoreException("Error instantiating class {}", classname, exception);
        }

        final MergeForgivenessStrategy strategy;
        try
        {
            strategy = (MergeForgivenessStrategy) constructor.newInstance(new Object[] {}); // NOSONAR
        }
        catch (final ClassCastException exception)
        {
            throw new CoreException("Class {} not a subtype of {}", classname,
                    MergeForgivenessStrategy.class.getName(), exception);
        }
        catch (final Exception exception)
        {
            throw new CoreException("Error instantiating class {}", classname, exception);
        }

        return Optional.of(strategy);
    }

    public ConfiguredMergeForgivenessPolicy()
    {
        this(new StandardConfiguration(new StringResource("{}")), GLOBAL_CONFIGURATION_ROOT, "");
    }

    private ConfiguredMergeForgivenessPolicy(final Configuration configuration,
            final String configurationRoot, final String owner)
    {
        try
        {
            final ConfigurationReader reader = new ConfigurationReader(configurationRoot);
            this.rootLevelFailurePolicy = parseRootLevelFailurePolicy(configuration, reader);
            this.subSequenceFailurePolicy = parseSubSequenceFailurePolicy(configuration, reader);
            this.exactSequenceFailurePolicy = parseExactSequenceFailurePolicy(configuration,
                    reader);
        }
        catch (final Exception exception)
        {
            throw new CoreException("Unable to create ConfiguredMergeForgivenessPolicy", exception);
        }
        this.owner = owner;
    }

    /**
     * Given some {@link FeatureChangeMergeException} and a left and right {@link FeatureChange},
     * attempt to apply the merge forgiveness policies represented by this object. Returns a merged
     * {@link FeatureChange} result if possible. This method will fail if the selected policy itself
     * fails. The policy selection will try each of the following policies in this order until it
     * finds one that is defined: 1) exactSequenceFailurePolicy, 2) subSequenceFailurePolicy, 3)
     * rootLevelFailurePolicy.
     *
     * @param mergeException
     *            the exception detailing the merge failure
     * @param left
     *            the left {@link FeatureChange}
     * @param right
     *            the right {@link FeatureChange}
     * @return the merged {@link FeatureChange}, wrapped in an {@link Optional}
     */
    public Optional<FeatureChange> applyPolicy(final FeatureChangeMergeException mergeException, // NOSONAR
            final FeatureChange left, final FeatureChange right)
    {
        for (final FailureSequencePolicyElement policyElement : this.exactSequenceFailurePolicy)
        {
            if (mergeException.traceMatchesExactFailureSequence(policyElement.getFailureSequence()))
            {
                try
                {
                    return Optional.of(policyElement.getStrategy().resolve(left, right,
                            policyElement.getConfiguration()));
                }
                catch (final Exception exception)
                {
                    throw new CoreException(
                            "Application of exactSequenceFailurePolicy failed for {} caused by:\n{}\nvs\n{}",
                            mergeException.getMergeFailureTrace(), left, right, exception);
                }
            }
        }

        for (final FailureSequencePolicyElement policyElement : this.subSequenceFailurePolicy)
        {
            if (mergeException
                    .traceContainsExactFailureSubSequence(policyElement.getFailureSequence()))
            {
                try
                {
                    return Optional.of(policyElement.getStrategy().resolve(left, right,
                            policyElement.getConfiguration()));
                }
                catch (final Exception exception)
                {
                    throw new CoreException(
                            "Application of subSequenceFailurePolicy failed for {} caused by:\n{}\nvs\n{}",
                            mergeException.getMergeFailureTrace(), left, right, exception);
                }
            }
        }

        for (final RootLevelPolicyElement policyElement : this.rootLevelFailurePolicy)
        {
            if (mergeException.rootLevelFailure() == policyElement.getRootLevelFailureType())
            {
                try
                {
                    return Optional.of(policyElement.getStrategy().resolve(left, right,
                            policyElement.getConfiguration()));
                }
                catch (final Exception exception)
                {
                    throw new CoreException(
                            "Application of rootLevelFailurePolicy failed for {} caused by:\n{}\nvs\n{}",
                            mergeException.getMergeFailureTrace(), left, right, exception);
                }
            }
        }

        logger.warn("No applicable merge resolution policy found for {} caused by:\n{}\nvs\n{}",
                mergeException.getMergeFailureTrace(), left, right);
        return Optional.empty();
    }

    public List<FailureSequencePolicyElement> getExactSequenceFailurePolicy()
    {
        return this.exactSequenceFailurePolicy;
    }

    public String getOwner()
    {
        return this.owner;
    }

    public List<RootLevelPolicyElement> getRootLevelFailurePolicy()
    {
        return this.rootLevelFailurePolicy;
    }

    public List<FailureSequencePolicyElement> getSubSequenceFailurePolicy()
    {
        return this.subSequenceFailurePolicy;
    }

    public boolean policyIsEmpty()
    {
        return this.rootLevelFailurePolicy.isEmpty() && this.subSequenceFailurePolicy.isEmpty()
                && this.exactSequenceFailurePolicy.isEmpty();
    }

    public JsonObject toJson()
    {
        final JsonObject policyObject = new JsonObject();
        policyObject.addProperty(AtlasMutatorConfiguration.TYPE_JSON_PROPERTY,
                TYPE_JSON_PROPERTY_VALUE);
        policyObject.addProperty(OWNER_JSON_PROPERTY, this.owner);

        final JsonArray rootLevelElements = new JsonArray();
        for (final RootLevelPolicyElement element : this.rootLevelFailurePolicy)
        {
            rootLevelElements.add(new JsonPrimitive(element.toString()));
        }
        if (!this.rootLevelFailurePolicy.isEmpty())
        {
            policyObject.add(ROOT_LEVEL_POLICY_JSON_PROPERTY, rootLevelElements);
        }

        final JsonArray subSequenceElements = new JsonArray();
        for (final FailureSequencePolicyElement element : this.subSequenceFailurePolicy)
        {
            subSequenceElements.add(new JsonPrimitive(element.toString()));
        }
        if (!this.subSequenceFailurePolicy.isEmpty())
        {
            policyObject.add(SUB_SEQUENCE_FAILURE_POLICY_JSON_PROPERTY, subSequenceElements);
        }

        final JsonArray exactSequenceElements = new JsonArray();
        for (final FailureSequencePolicyElement element : this.exactSequenceFailurePolicy)
        {
            exactSequenceElements.add(new JsonPrimitive(element.toString()));
        }
        if (!this.exactSequenceFailurePolicy.isEmpty())
        {
            policyObject.add(EXACT_SEQUENCE_FAILURE_POLICY_JSON_PROPERTY, exactSequenceElements);
        }

        return policyObject;
    }

    @Override
    public String toString()
    {

        return "ConfiguredMergeForgivenessPolicy {" + "\nrootLevelFailurePolicy="
                + this.rootLevelFailurePolicy + "\nexactSequenceFailurePolicy="
                + this.exactSequenceFailurePolicy + "\nsubSequenceFailurePolicy="
                + this.subSequenceFailurePolicy + "\n}";

    }

    private Map<String, Serializable> getIntermediateConfigurationMap(
            final Map<Object, Serializable> map)
    {
        if (map == null)
        {
            return new HashMap<>();
        }

        final Map<String, Serializable> result = new HashMap<>();
        for (final Map.Entry<Object, Serializable> entry : map.entrySet())
        {
            final String key = (String) entry.getKey();
            final Serializable value = entry.getValue();
            result.put(key, value);
        }
        return result;
    }

    private List<FailureSequencePolicyElement> parseExactSequenceFailurePolicy(
            final Configuration configuration, final ConfigurationReader reader)
    {
        return this.parseGenericSequencePolicy(configuration, reader,
                CONFIGURATION_RESOLVABLE_FAILURE_EXACT_SEQUENCES,
                CONFIGURATION_FAILURE_EXACT_SEQUENCE, true);
    }

    @SuppressWarnings("unchecked")
    private List<FailureSequencePolicyElement> parseGenericSequencePolicy(
            final Configuration configuration, final ConfigurationReader reader,
            final String sequenceType, final String sequenceSpecifier, final boolean exact)
    {
        try
        {
            final List<FailureSequencePolicyElement> parsedElements = new ArrayList<>();
            final List<Object> rawElements = reader.configurationValue(configuration, sequenceType,
                    new ArrayList<>());
            for (final Object rawElement : rawElements)
            {
                final Map<String, Object> map = (Map<String, Object>) rawElement;

                final List<String> rawList = (List<String>) map.get(sequenceSpecifier);
                final List<MergeFailureType> mergeFailureTypes = new ArrayList<>();
                for (final String typeString : rawList)
                {
                    mergeFailureTypes.add(MergeFailureType.valueOf(typeString));
                }

                final String strategyClassname = (String) map.get(CONFIGURATION_STRATEGY_CLASSNAME);
                final MergeForgivenessStrategy strategy = instantiateStrategy(strategyClassname)
                        .orElseThrow(() -> new CoreException("Could not instantiate class {}",
                                strategyClassname));

                final Map<String, Serializable> strategyConfiguration = getIntermediateConfigurationMap(
                        (Map<Object, Serializable>) map.get(CONFIGURATION_STRATEGY_CONFIGURATION));

                parsedElements.add(new FailureSequencePolicyElement(mergeFailureTypes, strategy,
                        strategyConfiguration, exact));
            }
            return parsedElements;
        }
        catch (final Exception exception)
        {
            throw new CoreException("Could not parse failure subsequence policy from {}",
                    CONFIGURATION_RESOLVABLE_FAILURE_SUBSEQUENCES);
        }
    }

    @SuppressWarnings("unchecked")
    private List<RootLevelPolicyElement> parseRootLevelFailurePolicy(
            final Configuration configuration, final ConfigurationReader reader)
    {
        try
        {
            final List<RootLevelPolicyElement> parsedElements = new ArrayList<>();
            final List<Object> rawElements = reader.configurationValue(configuration,
                    CONFIGURATION_RESOLVABLE_ROOT_LEVEL_FAILURES, new ArrayList<>());
            for (final Object rawElement : rawElements)
            {
                final Map<String, Object> map = (Map<String, Object>) rawElement;

                final MergeFailureType mergeFailureType = MergeFailureType
                        .valueOf((String) map.get(CONFIGURATION_ROOT_LEVEL_FAILURE));

                final String strategyClassname = (String) map.get(CONFIGURATION_STRATEGY_CLASSNAME);
                final MergeForgivenessStrategy strategy = instantiateStrategy(strategyClassname)
                        .orElseThrow(() -> new CoreException("Could not instantiate class {}",
                                strategyClassname));

                final Map<String, Serializable> strategyConfiguration = getIntermediateConfigurationMap(
                        (Map<Object, Serializable>) map.get(CONFIGURATION_STRATEGY_CONFIGURATION));

                parsedElements.add(new RootLevelPolicyElement(mergeFailureType, strategy,
                        strategyConfiguration));
            }
            return parsedElements;
        }
        catch (final Exception exception)
        {
            throw new CoreException("Could not parse root level failure policy from {}",
                    CONFIGURATION_RESOLVABLE_ROOT_LEVEL_FAILURES);
        }
    }

    private List<FailureSequencePolicyElement> parseSubSequenceFailurePolicy(
            final Configuration configuration, final ConfigurationReader reader)
    {
        return this.parseGenericSequencePolicy(configuration, reader,
                CONFIGURATION_RESOLVABLE_FAILURE_SUBSEQUENCES, CONFIGURATION_FAILURE_SUBSEQUENCE,
                false);
    }
}
