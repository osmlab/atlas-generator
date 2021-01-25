package org.openstreetmap.atlas.mutator.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.SlippyTileSharding;
import org.openstreetmap.atlas.mutator.AtlasMutatorParameters;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutationLevel;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.command.AtlasShellToolsException;
import org.openstreetmap.atlas.utilities.command.abstractcommand.AbstractAtlasShellToolsCommand;
import org.openstreetmap.atlas.utilities.command.abstractcommand.CommandOutputDelegate;
import org.openstreetmap.atlas.utilities.command.abstractcommand.OptionAndArgumentDelegate;
import org.openstreetmap.atlas.utilities.command.parsing.ArgumentArity;
import org.openstreetmap.atlas.utilities.command.parsing.ArgumentOptionality;
import org.openstreetmap.atlas.utilities.command.parsing.OptionOptionality;
import org.openstreetmap.atlas.utilities.command.terminal.TTYAttribute;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.tuples.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * @author lcram
 */
public class ConfigurationInfoCommand extends AbstractAtlasShellToolsCommand
{
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationInfoCommand.class);

    private static final String ALLOW_RDD_OPTION_LONG = "allowRDD";
    private static final String ALLOW_RDD_OPTION_DESCRIPTION = "Allow eligible mutation levels to source data from previous level's RDD.";

    private static final String PRELOAD_RDD_OPTION_LONG = "preloadRDD";
    private static final String PRELOAD_RDD_OPTION_DESCRIPTION = "Allow eligible mutation levels to preload atlas data into an RDD before processing mutations.";

    private static final String GROUP_COUNTRIES_OPTION_LONG = "group-countries";
    private static final String GROUP_COUNTRIES_OPTION_DESCRIPTION = "Allow the driver to group countries with similar level configurations together.";

    private static final String COUNTRIES_OPTION_LONG = "countries";
    private static final String COUNTRIES_OPTION_DESCRIPTION = "Comma separated list of countries for the mutator configuration.";
    private static final String COUNTRIES_OPTION_HINT = "country-list";

    private static final String CONFIGURATION_RESOURCE_OPTION_LONG = "config-resource";
    private static final String CONFIGURATION_RESOURCE_OPTION_DESCRIPTION = "Comma-separated list of configuration resources. These can be either http URLs or local files.";
    private static final String CONFIGURATION_RESOURCE_OPTION_HINT = "config-resources";

    private static final String CONFIGURATION_JSON_OPTION_LONG = "config-json";
    private static final String CONFIGURATION_JSON_OPTION_DESCRIPTION = "Read the configuration from JSON format.";
    private static final String CONFIGURATION_JSON_OPTION_HINT = "json";

    private static final String DISABLE_JLINE_OPTION_LONG = "disable-jline";
    private static final String DISABLE_JLINE_OPTION_DESCRIPTION = "Disable jLine input features in interactive mode. Try this if interactive mode causes issues for your terminal.";

    private static final String MODE_ARGUMENT_HINT = "mode";
    private static final String MODE_PRINT = "print";
    private static final String MODE_DETAILS = "details";
    private static final String MODE_INTERACTIVE = "interactive";
    private static final String ARGS_ARGUMENT_HINT = "mode-args";

    private static final String CONFIG_EMPTY_MESSAGE = "the AtlasMutatorConfiguration was empty";

    private final OptionAndArgumentDelegate optionAndArgumentDelegate;
    private final CommandOutputDelegate outputDelegate;
    /*
     * Save some jLine objects globally so their contents can be saved between readLine method
     * calls. This is not the cleanest solution but it's simple and it works.
     */
    private History history;
    private Terminal terminal;

    public static void main(final String[] args)
    {
        new ConfigurationInfoCommand().runSubcommandAndExit(args);
    }

    public ConfigurationInfoCommand()
    {
        this.optionAndArgumentDelegate = this.getOptionAndArgumentDelegate();
        this.outputDelegate = this.getCommandOutputDelegate();
    }

    @Override
    public int execute()
    {
        /*
         * Read countries parameter.
         */
        final String countriesString = this.optionAndArgumentDelegate
                .getOptionArgument(COUNTRIES_OPTION_LONG)
                .orElseThrow(AtlasShellToolsException::new);
        final Set<String> countries = this.parseCommaSeparatedStrings(countriesString).stream()
                .collect(Collectors.toSet());

        /*
         * Construct the configuration.
         */
        if (this.optionAndArgumentDelegate.hasVerboseOption())
        {
            this.outputDelegate.printlnCommandMessage("loading config resources...");
        }
        final String configResource = this.optionAndArgumentDelegate
                .getOptionArgument(CONFIGURATION_RESOURCE_OPTION_LONG).orElse("");
        final String configJson = this.optionAndArgumentDelegate
                .getOptionArgument(CONFIGURATION_JSON_OPTION_LONG).orElse("");
        if (configJson.isEmpty() && configResource.isEmpty())
        {
            this.outputDelegate.printlnErrorMessage("no configurations were supplied");
            return 1;
        }
        StringList configList = new StringList();
        if (!configResource.isEmpty())
        {
            configList = StringList.split(configResource, ",");
        }
        final Optional<Configuration> configOptional;
        try
        {
            configOptional = AtlasMutatorParameters.mutatorsConfiguration(configList, configJson,
                    this.getFileSystem());
        }
        catch (final Exception exception)
        {
            logger.error("Could not fetch Configuration object using configList {}", configList,
                    exception);
            this.outputDelegate.printlnErrorMessage(
                    "failed to fetch Configuration object, please check logs for details");
            return 1;
        }
        if (configOptional.isEmpty())
        {
            this.outputDelegate.printlnErrorMessage(
                    "no configurations could be processed, please check logs for details");
            return 1;
        }
        final Configuration configuration = configOptional.get();
        final Set<String> configDataKeySet = configuration.configurationDataKeySet();
        if (configDataKeySet.isEmpty())
        {
            this.outputDelegate.printlnErrorMessage(
                    "supplied configuration was empty, please check logs for details");
            return 1;
        }
        if (configDataKeySet.size() == 1 && "404".equals(configDataKeySet.iterator().next()))
        {
            this.outputDelegate.printlnErrorMessage(
                    "apparent 404 error when fetching configuration, please check the resource URL");
            return 1;
        }

        /*
         * Run the command based on the mode.
         */
        final String mode = this.optionAndArgumentDelegate.getUnaryArgument(MODE_ARGUMENT_HINT)
                .orElseThrow(AtlasShellToolsException::new);
        if (MODE_PRINT.equals(mode))
        {
            return executePrintMode(countries, configuration);
        }
        else if (MODE_DETAILS.equals(mode))
        {
            return executeDetailsMode(countries, configuration);
        }
        else if (MODE_INTERACTIVE.equals(mode))
        {
            return executeInteractiveMode(countries, configuration);
        }
        else
        {
            this.outputDelegate
                    .printlnErrorMessage(String.format("unknown mode '%s': try '%s', '%s', or '%s'",
                            mode, MODE_PRINT, MODE_DETAILS, MODE_INTERACTIVE));
            return 1;
        }
    }

    @Override
    public String getCommandName()
    {
        return "config-info";
    }

    @Override
    public String getSimpleDescription()
    {
        return "get information about an atlas-mutator configuration";
    }

    @Override
    public void registerManualPageSections()
    {
        addManualPageSection("DESCRIPTION", ConfigurationInfoCommand.class
                .getResourceAsStream("ConfigurationInfoCommandDescriptionSection.txt"));
        addManualPageSection("EXAMPLES", ConfigurationInfoCommand.class
                .getResourceAsStream("ConfigurationInfoCommandExamplesSection.txt"));
    }

    @Override
    public void registerOptionsAndArguments()
    {
        registerOption(ALLOW_RDD_OPTION_LONG, ALLOW_RDD_OPTION_DESCRIPTION,
                OptionOptionality.OPTIONAL);
        registerOption(PRELOAD_RDD_OPTION_LONG, PRELOAD_RDD_OPTION_DESCRIPTION,
                OptionOptionality.OPTIONAL);
        registerOption(GROUP_COUNTRIES_OPTION_LONG, GROUP_COUNTRIES_OPTION_DESCRIPTION,
                OptionOptionality.OPTIONAL);
        registerOptionWithRequiredArgument(COUNTRIES_OPTION_LONG, COUNTRIES_OPTION_DESCRIPTION,
                OptionOptionality.REQUIRED, COUNTRIES_OPTION_HINT);
        registerOptionWithRequiredArgument(CONFIGURATION_RESOURCE_OPTION_LONG,
                CONFIGURATION_RESOURCE_OPTION_DESCRIPTION, OptionOptionality.OPTIONAL,
                CONFIGURATION_RESOURCE_OPTION_HINT);
        registerOptionWithRequiredArgument(CONFIGURATION_JSON_OPTION_LONG,
                CONFIGURATION_JSON_OPTION_DESCRIPTION, OptionOptionality.OPTIONAL,
                CONFIGURATION_JSON_OPTION_HINT);
        registerOption(DISABLE_JLINE_OPTION_LONG, DISABLE_JLINE_OPTION_DESCRIPTION,
                OptionOptionality.OPTIONAL);
        registerArgument(MODE_ARGUMENT_HINT, ArgumentArity.UNARY, ArgumentOptionality.REQUIRED);
        registerArgument(ARGS_ARGUMENT_HINT, ArgumentArity.VARIADIC, ArgumentOptionality.OPTIONAL);
        super.registerOptionsAndArguments();
    }

    private boolean containsAllTerms(final String string, final List<String> terms)
    {
        for (final String term : terms)
        {
            if (!string.contains(term))
            {
                return false;
            }
        }
        return true;
    }

    private int executeDetailsMode(final Set<String> countries, final Configuration configuration)
    {
        final List<String> searchTerms = this.optionAndArgumentDelegate
                .getVariadicArgument(ARGS_ARGUMENT_HINT);

        final Optional<AtlasMutatorConfiguration> atlasMutatorConfigurationOptional = getConfiguration(
                countries, configuration);
        if (atlasMutatorConfigurationOptional.isEmpty())
        {
            this.outputDelegate.printlnErrorMessage(CONFIG_EMPTY_MESSAGE);
            return 1;
        }
        final JsonObject configObject = atlasMutatorConfigurationOptional.get().toJson();
        if (searchTerms.isEmpty())
        {
            this.outputDelegate.printlnStdout(getPrettyGson().toJson(configObject));
            return 0;
        }
        searchJsonObjectForTermsAndPrint(configObject, searchTerms);

        return 0;
    }

    private int executeInteractiveMode(final Set<String> countries,
            final Configuration configuration)
    {
        this.outputDelegate.printlnCommandMessage("starting interactive mode...");
        final Optional<AtlasMutatorConfiguration> atlasMutatorConfigurationOptional = getConfiguration(
                countries, configuration);
        if (atlasMutatorConfigurationOptional.isEmpty())
        {
            this.outputDelegate.printlnErrorMessage(CONFIG_EMPTY_MESSAGE);
            return 1;
        }
        final AtlasMutatorConfiguration atlasMutatorConfiguration = atlasMutatorConfigurationOptional
                .get();
        final Set<String> configurationKeys = atlasMutatorConfiguration.getConfigurationDetails()
                .getAllConfigurationMapKeys();
        final JsonObject configObject = atlasMutatorConfiguration.toJson();

        this.outputDelegate.printlnStdout("");
        this.outputDelegate.printlnStdout(
                "-------- Atlas Mutator Configuration Interactive Exploration --------",
                TTYAttribute.BOLD, TTYAttribute.GREEN);
        this.outputDelegate.printlnStdout("Please enter search terms separated by spaces.");
        this.outputDelegate.printlnStdout("");
        this.outputDelegate.printlnStdout("--- Controls ---", TTYAttribute.BOLD);
        if (this.optionAndArgumentDelegate.hasOption(DISABLE_JLINE_OPTION_LONG))
        {
            this.outputDelegate.printlnStdout("CTRL-D: exit");
            this.outputDelegate.printlnStdout("CTRL-C: exit");
        }
        else
        {
            this.outputDelegate.printlnStdout("CTRL-D: exit");
            this.outputDelegate.printlnStdout("CTRL-C: reset prompt");
            this.outputDelegate.printlnStdout("CTRL-L: clear screen");
            this.outputDelegate.printlnStdout("CTRL-R: history search");
            this.outputDelegate.printlnStdout("Arrow keys: navigate history or current line");
            this.outputDelegate.printlnStdout("<TAB>: autocomplete current line");
        }

        final String prompt = this.getTTYStringBuilderForStdout()
                .append("search terms", TTYAttribute.BOLD, TTYAttribute.BLUE)
                .append("> ", TTYAttribute.BOLD, TTYAttribute.RED).toString();
        while (true)
        {
            final String readLine;
            if (this.optionAndArgumentDelegate.hasOption(DISABLE_JLINE_OPTION_LONG))
            {
                readLine = readLineWithJavaIO(prompt);
            }
            else
            {
                readLine = readLineWithJLine(prompt, configurationKeys);
            }
            if (readLine == null)
            {
                break;
            }
            final String[] lineSplit = readLine.split("\\s+");
            searchJsonObjectForTermsAndPrint(configObject, Arrays.asList(lineSplit));
        }

        /*
         * If the terminal was initialized, we need to shut it down.
         */
        if (this.terminal != null)
        {
            try
            {
                this.terminal.close();
            }
            catch (final IOException exception)
            {
                throw new CoreException("Could not close terminal", exception);
            }
        }

        return 0;
    }

    private int executePrintMode(final Set<String> countries, final Configuration configuration)
    {
        List<String> countryCodesToPrint = this.optionAndArgumentDelegate
                .getVariadicArgument(ARGS_ARGUMENT_HINT);
        final Optional<AtlasMutatorConfiguration> atlasMutatorConfigurationOptional = getConfiguration(
                countries, configuration);
        if (atlasMutatorConfigurationOptional.isEmpty())
        {
            this.outputDelegate.printlnErrorMessage(CONFIG_EMPTY_MESSAGE);
            return 1;
        }
        final AtlasMutatorConfiguration atlasMutatorConfiguration = atlasMutatorConfigurationOptional
                .get();
        if (countryCodesToPrint.isEmpty())
        {
            countryCodesToPrint = new ArrayList<>(
                    new TreeSet<>(atlasMutatorConfiguration.getCountryToMutationLevels().keySet()));
        }
        for (final String countryOrGroupCode : countryCodesToPrint)
        {
            final Tuple<Tuple<String, Set<String>>, List<AtlasMutationLevel>> countryOrGroupAndLevels = atlasMutatorConfiguration
                    .countryOrGroupAndLevelList(countryOrGroupCode);
            final Tuple<Tuple<String, Set<String>>, String> countryOrGroupAndDetails = atlasMutatorConfiguration
                    .details(countryOrGroupCode);
            if (countryOrGroupAndDetails != null)
            {
                final Tuple<String, Set<String>> countryOrGroup = countryOrGroupAndLevels
                        .getFirst();
                this.outputDelegate.printStdout("Level information for ", TTYAttribute.BOLD);
                this.outputDelegate.printlnStdout(
                        countryOrGroup.getFirst() + " ("
                                + new StringList(countryOrGroup.getSecond()).join(",") + ")",
                        TTYAttribute.BOLD, TTYAttribute.GREEN);
                this.outputDelegate.printlnStdout(countryOrGroupAndDetails.getSecond(),
                        TTYAttribute.GREEN);
            }
            else
            {
                this.outputDelegate.printlnWarnMessage(
                        countryOrGroupCode + " was not found in the configuration");
            }
            this.outputDelegate.printlnStdout("");
        }
        return 0;
    }

    private Optional<AtlasMutatorConfiguration> getConfiguration(final Set<String> countries,
            final Configuration configuration)
    {
        // Sharding, boundary map, input, output, and spark config are not necessary for our
        // purposes, so we can fill in dummy values here. We also disable boundary validation since
        // there are none.
        if (this.optionAndArgumentDelegate.hasVerboseOption())
        {
            this.outputDelegate.printlnCommandMessage("constructing configuration...");
        }
        final AtlasMutatorConfiguration mutatorConfiguration = new AtlasMutatorConfiguration(
                countries, new SlippyTileSharding(1), new CountryBoundaryMap(), "input", "output",
                new HashMap<>(), configuration,
                this.optionAndArgumentDelegate.hasOption(GROUP_COUNTRIES_OPTION_LONG),
                this.optionAndArgumentDelegate.hasOption(ALLOW_RDD_OPTION_LONG),
                this.optionAndArgumentDelegate.hasOption(PRELOAD_RDD_OPTION_LONG), false);
        if (mutatorConfiguration.isEmpty())
        {
            return Optional.empty();
        }
        return Optional.of(mutatorConfiguration);
    }

    private Gson getPrettyGson()
    {
        return new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
    }

    private StringList parseCommaSeparatedStrings(final String string)
    {
        final StringList stringList = new StringList();

        if (string.isEmpty())
        {
            return stringList;
        }

        stringList.addAll(Arrays.stream(string.split(",")).collect(Collectors.toList()));
        return stringList;
    }

    private String readLineWithJLine(final String prompt, final Set<String> configurationKeys)
    {
        if (this.history == null)
        {
            this.history = new DefaultHistory();
        }
        try
        {
            this.terminal = TerminalBuilder.builder().build();
        }
        catch (final IOException exception)
        {
            throw new CoreException("Could not build terminal", exception);
        }
        final StringsCompleter completer = new StringsCompleter(configurationKeys);
        final LineReader reader = LineReaderBuilder.builder().history(this.history)
                .completer(completer).terminal(this.terminal).build();
        String line;
        while (true) // NOSONAR
        {
            try
            {
                line = reader.readLine(prompt);
            }
            catch (final UserInterruptException exception)
            {
                continue;
            }
            catch (final EndOfFileException exception)
            {
                return null;
            }
            break;
        }
        return line;
    }

    private String readLineWithJavaIO(final String prompt)
    {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(this.getInStream()));
        this.outputDelegate.printStdout(prompt);
        try
        {
            return reader.readLine();
        }
        catch (final IOException exception)
        {
            throw new CoreException("Could not read line from InputStream", exception);
        }
    }

    private void searchForMatchAndPrint(final JsonElement element, final List<String> searchTerms)
    {
        final String elementString = element.toString();
        if (containsAllTerms(elementString, searchTerms))
        {
            String prettyElement = getPrettyGson().toJson(element);
            for (final String searchTerm : searchTerms)
            {
                prettyElement = prettyElement.replace(searchTerm,
                        this.getTTYStringBuilderForStdout()
                                .append(searchTerm, TTYAttribute.RED, TTYAttribute.BOLD)
                                .toString());
            }
            this.outputDelegate.printlnStdout(prettyElement);
        }
    }

    private void searchJsonObjectForTermsAndPrint(final JsonObject configObject,
            final List<String> searchTerms)
    {
        for (final JsonElement element : configObject
                .getAsJsonArray(AtlasMutatorConfiguration.MUTATORS_JSON_PROPERTY))
        {
            searchForMatchAndPrint(element, searchTerms);
        }
        for (final JsonElement element : configObject
                .getAsJsonArray(AtlasMutatorConfiguration.LEVELS_JSON_PROPERTY))
        {
            searchForMatchAndPrint(element, searchTerms);
        }
        for (final JsonElement element : configObject
                .getAsJsonArray(AtlasMutatorConfiguration.DYNAMIC_ATLAS_POLICIES_JSON_PROPERTY))
        {
            searchForMatchAndPrint(element, searchTerms);
        }
        for (final JsonElement element : configObject
                .getAsJsonArray(AtlasMutatorConfiguration.FILTERS_JSON_PROPERTY))
        {
            searchForMatchAndPrint(element, searchTerms);
        }
        for (final JsonElement element : configObject
                .getAsJsonArray(AtlasMutatorConfiguration.INPUT_DEPENDENCIES_JSON_PROPERTY))
        {
            searchForMatchAndPrint(element, searchTerms);
        }
        for (final JsonElement element : configObject
                .getAsJsonArray(AtlasMutatorConfiguration.FETCHERS_JSON_PROPERTY))
        {
            searchForMatchAndPrint(element, searchTerms);
        }
        for (final JsonElement element : configObject
                .getAsJsonArray(AtlasMutatorConfiguration.SUB_ATLASES_JSON_PROPERTY))
        {
            searchForMatchAndPrint(element, searchTerms);
        }
        for (final JsonElement element : configObject
                .getAsJsonArray(AtlasMutatorConfiguration.MERGE_FORGIVENESS_POLICIES_JSON_PROPERTY))
        {
            searchForMatchAndPrint(element, searchTerms);
        }
        for (final JsonElement element : configObject
                .getAsJsonArray(AtlasMutatorConfiguration.BROADCASTABLES_JSON_PROPERTY))
        {
            searchForMatchAndPrint(element, searchTerms);
        }
    }
}
