/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.config;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * This class is used for specifying the set of expected configurations. For each configuration, you can specify
 * the name, the type, the default value, the documentation, the group information, the order in the group,
 * the width of the configuration value and the name suitable for display in the UI.
 *
 * You can provide special validation logic used for single configuration validation by overriding {@link Validator}.
 *
 * Moreover, you can specify the dependents of a configuration. The valid values and visibility of a configuration
 * may change according to the values of other configurations. You can override {@link Recommender} to get valid
 * values and set visibility of a configuration given the current configuration values.
 *
 * <p/>
 * To use the class:
 * <p/>
 * <pre>
 * ConfigDef defs = new ConfigDef();
 *
 * defs.define(&quot;config_with_default&quot;, Type.STRING, &quot;default string value&quot;, &quot;Configuration with default value.&quot;);
 * defs.define(&quot;config_with_validator&quot;, Type.INT, 42, Range.atLeast(0), &quot;Configuration with user provided validator.&quot;);
 * defs.define(&quot;config_with_dependents&quot;, Type.INT, &quot;Configuration with dependents.&quot;, &quot;group&quot;, 1, &quot;Config With Dependents&quot;, Arrays.asList(&quot;config_with_default;&quot;,&quot;config_with_validator&quot;));
 *
 * Map&lt;String, String&gt; props = new HashMap&lt;&gt();
 * props.put(&quot;config_with_default&quot;, &quot;some value&quot;);
 * props.put(&quot;config_with_dependents&quot;, &quot;some other value&quot;);
 * // will return &quot;some value&quot;
 * Map&lt;String, Object&gt; configs = defs.parse(props);
 * String someConfig = (String) configs.get(&quot;config_with_default&quot;);
 * // will return default value of 42
 * int anotherConfig = (Integer) configs.get(&quot;config_with_validator&quot;);
 *
 * To validate the full configuration, use:
 * List&lt;Config&gt; configs = def.validate(props);
 * The {@link Config} contains updated configuration information given the current configuration values.
 * </pre>
 * <p/>
 * This class can be used standalone or in combination with {@link AbstractConfig} which provides some additional
 * functionality for accessing configs.
 */
public class ConfigDef {

    public static final Object NO_DEFAULT_VALUE = new String("");

    private final Map<String, ConfigKey> configKeys = new HashMap<>();
    private final List<String> groups = new LinkedList<>();
    private Set<String> configsWithNoParent;

    /**
     * Returns unmodifiable set of properties names defined in this {@linkplain ConfigDef}
     *
     * @return new unmodifiable {@link Set} instance containing the keys
     */
    public Set<String> names() {
        return Collections.unmodifiableSet(configKeys.keySet());
    }

    /**
     * Define a new configuration
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param validator     the validator to use in checking the correctness of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @param recommender   the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents, Recommender recommender) {
        if (configKeys.containsKey(name)) {
            throw new ConfigException("Configuration " + name + " is defined twice.");
        }
        if (group != null && !groups.contains(group)) {
            groups.add(group);
        }
        Object parsedDefault = defaultValue == NO_DEFAULT_VALUE ? NO_DEFAULT_VALUE : parseType(name, defaultValue, type);
        configKeys.put(name, new ConfigKey(name, type, parsedDefault, validator, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender));
        return this;
    }

    /**
     * Define a new configuration with no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param validator     the validator to use in checking the correctness of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    /**
     * Define a new configuration with no dependents
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param validator     the validator to use in checking the correctness of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param recommender   the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    /**
     * Define a new configuration with no dependents and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param validator     the validator to use in checking the correctness of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName) {
        return define(name, type, defaultValue, validator, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    /**
     * Define a new configuration with no special validation logic
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @param recommender   the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents, Recommender recommender) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender);
    }

    /**
     * Define a new configuration with no special validation logic and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, List<String> dependents) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    /**
     * Define a new configuration with no special validation logic and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param recommender   the recommender provides valid values given the parent configuration values
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName, Recommender recommender) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    /**
     * Define a new configuration with no special validation logic, not dependents and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation,
                            String group, int orderInGroup, Width width, String displayName) {
        return define(name, type, defaultValue, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    /**
     * Define a new configuration with no default value and no special validation logic
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @param recommender   the recommender provides valid values given the parent configuration value
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, List<String> dependents, Recommender recommender) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, dependents, recommender);
    }

    /**
     * Define a new configuration with no default value, no special validation logic and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param dependents    the configurations that are dependents of this configuration
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, List<String> dependents) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, dependents, null);
    }

    /**
     * Define a new configuration with no default value, no special validation logic and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @param recommender   the recommender provides valid values given the parent configuration value
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName, Recommender recommender) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList(), recommender);
    }

    /**
     * Define a new configuration with no default value, no special validation logic, no dependents and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @param group         the group this config belongs to
     * @param orderInGroup  the order of this config in the group
     * @param width         the width of the config
     * @param displayName   the name suitable for display
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation, String group, int orderInGroup,
                            Width width, String displayName) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation, group, orderInGroup, width, displayName, Collections.<String>emptyList());
    }

    /**
     * Define a new configuration with no group, no order in group, no width, no display name, no dependents and no custom recommender
     * @param name          the name of the config parameter
     * @param type          the type of the config
     * @param defaultValue  the default value to use if this config isn't present
     * @param validator     the validator to use in checking the correctness of the config
     * @param importance    the importance of this config
     * @param documentation the documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Validator validator, Importance importance, String documentation) {
        return define(name, type, defaultValue, validator, importance, documentation, null, -1, Width.NONE, name);
    }

    /**
     * Define a new configuration with no special validation logic
     * @param name          The name of the config parameter
     * @param type          The type of the config
     * @param defaultValue  The default value to use if this config isn't present
     * @param importance    The importance of this config: is this something you will likely need to change.
     * @param documentation The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Object defaultValue, Importance importance, String documentation) {
        return define(name, type, defaultValue, null, importance, documentation);
    }

    /**
     * Define a new configuration with no default value and no special validation logic
     * @param name          The name of the config parameter
     * @param type          The type of the config
     * @param importance    The importance of this config: is this something you will likely need to change.
     * @param documentation The documentation string for the config
     * @return This ConfigDef so you can chain calls
     */
    public ConfigDef define(String name, Type type, Importance importance, String documentation) {
        return define(name, type, NO_DEFAULT_VALUE, null, importance, documentation);
    }

    /**
     * Get the configuration keys
     * @return a map containing all configuration keys
     */
    public Map<String, ConfigKey> configKeys() {
        return configKeys;
    }

    /**
     * Get the groups for the configuration
     * @return a list of group names
     */
    public List<String> groups() {
        return groups;
    }

    /**
     * Add standard SSL client configuration options.
     * @return this
     */
    public ConfigDef withClientSslSupport() {
        SslConfigs.addClientSslSupport(this);
        return this;
    }

    /**
     * Add standard SASL client configuration options.
     * @return this
     */
    public ConfigDef withClientSaslSupport() {
        SaslConfigs.addClientSaslSupport(this);
        return this;
    }

    /**
     * Parse and validate configs against this configuration definition. The input is a map of configs. It is expected
     * that the keys of the map are strings, but the values can either be strings or they may already be of the
     * appropriate type (int, string, etc). This will work equally well with either java.util.Properties instances or a
     * programmatically constructed map.
     *
     * @param props The configs to parse and validate.
     * @return Parsed and validated configs. The key will be the config name and the value will be the value parsed into
     * the appropriate type (int, string, etc).
     */
    public Map<String, Object> parse(Map<?, ?> props) {
        // Check all configurations are defined
        List<String> undefinedConfigKeys = undefinedDependentConfigs();
        if (!undefinedConfigKeys.isEmpty()) {
            String joined = Utils.join(undefinedConfigKeys, ",");
            throw new ConfigException("Some configurations in are referred in the dependents, but not defined: " + joined);
        }
        // parse all known keys
        Map<String, Object> values = new HashMap<>();
        for (ConfigKey key : configKeys.values()) {
            Object value;
            // props map contains setting - assign ConfigKey value
            if (props.containsKey(key.name)) {
                value = parseType(key.name, props.get(key.name), key.type);
                // props map doesn't contain setting, the key is required because no default value specified - its an error
            } else if (key.defaultValue == NO_DEFAULT_VALUE) {
                throw new ConfigException("Missing required configuration \"" + key.name + "\" which has no default value.");
            } else {
                // otherwise assign setting its default value
                value = key.defaultValue;
            }
            if (key.validator != null) {
                key.validator.ensureValid(key.name, value);
            }
            values.put(key.name, value);
        }
        return values;
    }

    /**
     * Validate the current configuration values with the configuration definition.
     * @param props the current configuration values
     * @return List of Config, each Config contains the updated configuration information given
     * the current configuration values.
     */
    public List<ConfigValue> validate(Map<String, String> props) {
        Map<String, ConfigValue> configValues = new HashMap<>();
        for (String name: configKeys.keySet()) {
            configValues.put(name, new ConfigValue(name));
        }

        List<String> undefinedConfigKeys = undefinedDependentConfigs();
        for (String undefinedConfigKey: undefinedConfigKeys) {
            ConfigValue undefinedConfigValue = new ConfigValue(undefinedConfigKey);
            undefinedConfigValue.addErrorMessage(undefinedConfigKey + " is referred in the dependents, but not defined.");
            undefinedConfigValue.visible(false);
            configValues.put(undefinedConfigKey, undefinedConfigValue);
        }

        Map<String, Object> parsed = parseForValidate(props, configValues);
        return validate(parsed, configValues);
    }

    // package accessible for testing
    Map<String, Object> parseForValidate(Map<String, String> props, Map<String, ConfigValue> configValues) {
        Map<String, Object> parsed = new HashMap<>();
        Set<String> configsWithNoParent = getConfigsWithNoParent();
        for (String name: configsWithNoParent) {
            parseForValidate(name, props, parsed, configValues);
        }
        return parsed;
    }


    private List<ConfigValue> validate(Map<String, Object> parsed, Map<String, ConfigValue> configValues) {
        Set<String> configsWithNoParent = getConfigsWithNoParent();
        for (String name: configsWithNoParent) {
            validate(name, parsed, configValues);
        }
        return new LinkedList<>(configValues.values());
    }

    private List<String> undefinedDependentConfigs() {
        Set<String> undefinedConfigKeys = new HashSet<>();
        for (String configName: configKeys.keySet()) {
            ConfigKey configKey = configKeys.get(configName);
            List<String> dependents = configKey.dependents;
            for (String dependent: dependents) {
                if (!configKeys.containsKey(dependent)) {
                    undefinedConfigKeys.add(dependent);
                }
            }
        }
        return new LinkedList<>(undefinedConfigKeys);
    }

    private Set<String> getConfigsWithNoParent() {
        if (this.configsWithNoParent != null) {
            return this.configsWithNoParent;
        }
        Set<String> configsWithParent = new HashSet<>();

        for (ConfigKey configKey: configKeys.values()) {
            List<String> dependents = configKey.dependents;
            configsWithParent.addAll(dependents);
        }

        Set<String> configs = new HashSet<>(configKeys.keySet());
        configs.removeAll(configsWithParent);
        this.configsWithNoParent = configs;
        return configs;
    }

    private void parseForValidate(String name, Map<String, String> props, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
        if (!configKeys.containsKey(name)) {
            return;
        }
        ConfigKey key = configKeys.get(name);
        ConfigValue config = configs.get(name);

        Object value = null;
        if (props.containsKey(key.name)) {
            try {
                value = parseType(key.name, props.get(key.name), key.type);
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        } else if (key.defaultValue == NO_DEFAULT_VALUE) {
            config.addErrorMessage("Missing required configuration \"" + key.name + "\" which has no default value.");
        } else {
            value = key.defaultValue;
        }

        if (key.validator != null) {
            try {
                key.validator.ensureValid(key.name, value);
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        }
        config.value(value);
        parsed.put(name, value);
        for (String dependent: key.dependents) {
            parseForValidate(dependent, props, parsed, configs);
        }
    }

    @SuppressWarnings("unchecked")
    private void validate(String name, Map<String, Object> parsed, Map<String, ConfigValue> configs) {
        if (!configKeys.containsKey(name)) {
            return;
        }
        ConfigKey key = configKeys.get(name);
        ConfigValue config = configs.get(name);
        List<Object> recommendedValues;
        if (key.recommender != null) {
            try {
                recommendedValues = key.recommender.validValues(name, parsed);
                List<Object> originalRecommendedValues = config.recommendedValues();
                if (!originalRecommendedValues.isEmpty()) {
                    Set<Object> originalRecommendedValueSet = new HashSet<>(originalRecommendedValues);
                    Iterator<Object> it = recommendedValues.iterator();
                    while (it.hasNext()) {
                        Object o = it.next();
                        if (!originalRecommendedValueSet.contains(o)) {
                            it.remove();
                        }
                    }
                }
                config.recommendedValues(recommendedValues);
                config.visible(key.recommender.visible(name, parsed));
            } catch (ConfigException e) {
                config.addErrorMessage(e.getMessage());
            }
        }

        configs.put(name, config);
        for (String dependent: key.dependents) {
            validate(dependent, parsed, configs);
        }
    }

    /**
     * Parse a value according to its expected type.
     * @param name  The config name
     * @param value The config value
     * @param type  The expected type
     * @return The parsed object
     */
    private Object parseType(String name, Object value, Type type) {
        try {
            if (value == null) return null;

            String trimmed = null;
            if (value instanceof String)
                trimmed = ((String) value).trim();

            switch (type) {
                case BOOLEAN:
                    if (value instanceof String) {
                        if (trimmed.equalsIgnoreCase("true"))
                            return true;
                        else if (trimmed.equalsIgnoreCase("false"))
                            return false;
                        else
                            throw new ConfigException(name, value, "Expected value to be either true or false");
                    } else if (value instanceof Boolean)
                        return value;
                    else
                        throw new ConfigException(name, value, "Expected value to be either true or false");
                case PASSWORD:
                    if (value instanceof Password)
                        return value;
                    else if (value instanceof String)
                        return new Password(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                case STRING:
                    if (value instanceof String)
                        return trimmed;
                    else
                        throw new ConfigException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
                case INT:
                    if (value instanceof Integer) {
                        return (Integer) value;
                    } else if (value instanceof String) {
                        return Integer.parseInt(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be an number.");
                    }
                case SHORT:
                    if (value instanceof Short) {
                        return (Short) value;
                    } else if (value instanceof String) {
                        return Short.parseShort(trimmed);
                    } else {
                        throw new ConfigException(name, value, "Expected value to be an number.");
                    }
                case LONG:
                    if (value instanceof Integer)
                        return ((Integer) value).longValue();
                    if (value instanceof Long)
                        return (Long) value;
                    else if (value instanceof String)
                        return Long.parseLong(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be an number.");
                case DOUBLE:
                    if (value instanceof Number)
                        return ((Number) value).doubleValue();
                    else if (value instanceof String)
                        return Double.parseDouble(trimmed);
                    else
                        throw new ConfigException(name, value, "Expected value to be an number.");
                case LIST:
                    if (value instanceof List)
                        return (List<?>) value;
                    else if (value instanceof String)
                        if (trimmed.isEmpty())
                            return Collections.emptyList();
                        else
                            return Arrays.asList(trimmed.split("\\s*,\\s*", -1));
                    else
                        throw new ConfigException(name, value, "Expected a comma separated list.");
                case CLASS:
                    if (value instanceof Class)
                        return (Class<?>) value;
                    else if (value instanceof String)
                        return Class.forName(trimmed, true, Utils.getContextOrKafkaClassLoader());
                    else
                        throw new ConfigException(name, value, "Expected a Class instance or class name.");
                default:
                    throw new IllegalStateException("Unknown type.");
            }
        } catch (NumberFormatException e) {
            throw new ConfigException(name, value, "Not a number of type " + type);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(name, value, "Class " + value + " could not be found.");
        }
    }

    public static String convertToString(Object parsedValue, Type type) {
        if (parsedValue == null) {
            return null;
        }

        if (type == null) {
            return parsedValue.toString();
        }

        switch (type) {
            case BOOLEAN:
            case SHORT:
            case INT:
            case LONG:
            case DOUBLE:
            case STRING:
            case PASSWORD:
                return parsedValue.toString();
            case LIST:
                List<?> valueList = (List<?>) parsedValue;
                return Utils.join(valueList, ",");
            case CLASS:
                Class<?> clazz = (Class<?>) parsedValue;
                return clazz.getCanonicalName();
            default:
                throw new IllegalStateException("Unknown type.");
        }
    }

    /**
     * The config types
     */
    public enum Type {
        BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD
    }

    /**
     * The importance level for a configuration
     */
    public enum Importance {
        HIGH, MEDIUM, LOW
    }

    /**
     * The width of a configuration value
     */
    public enum Width {
        NONE, SHORT, MEDIUM, LONG
    }

    /**
     * This is used by the {@link #validate(Map)} to get valid values for a configuration given the current
     * configuration values in order to perform full configuration validation and visibility modification.
     * In case that there are dependencies between configurations, the valid values and visibility
     * for a configuration may change given the values of other configurations.
     */
    public interface Recommender {

        /**
         * The valid values for the configuration given the current configuration values.
         * @param name The name of the configuration
         * @param parsedConfig The parsed configuration values
         * @return The list of valid values. To function properly, the returned objects should have the type
         * defined for the configuration using the recommender.
         */
        List<Object> validValues(String name, Map<String, Object> parsedConfig);

        /**
         * Set the visibility of the configuration given the current configuration values.
         * @param name The name of the configuration
         * @param parsedConfig The parsed configuration values
         * @return The visibility of the configuration
         */
        boolean visible(String name, Map<String, Object> parsedConfig);
    }

    /**
     * Validation logic the user may provide to perform single configuration validation.
     */
    public interface Validator {
        /**
         * Perform single configuration validation.
         * @param name The name of the configuration
         * @param value The value of the configuration
         */
        void ensureValid(String name, Object value);
    }

    /**
     * Validation logic for numeric ranges
     */
    public static class Range implements Validator {
        private final Number min;
        private final Number max;

        private Range(Number min, Number max) {
            this.min = min;
            this.max = max;
        }

        /**
         * A numeric range that checks only the lower bound
         *
         * @param min The minimum acceptable value
         */
        public static Range atLeast(Number min) {
            return new Range(min, null);
        }

        /**
         * A numeric range that checks both the upper and lower bound
         */
        public static Range between(Number min, Number max) {
            return new Range(min, max);
        }

        public void ensureValid(String name, Object o) {
            if (o == null)
                throw new ConfigException(name, o, "Value must be non-null");
            Number n = (Number) o;
            if (min != null && n.doubleValue() < min.doubleValue())
                throw new ConfigException(name, o, "Value must be at least " + min);
            if (max != null && n.doubleValue() > max.doubleValue())
                throw new ConfigException(name, o, "Value must be no more than " + max);
        }

        public String toString() {
            if (min == null)
                return "[...," + max + "]";
            else if (max == null)
                return "[" + min + ",...]";
            else
                return "[" + min + ",...," + max + "]";
        }
    }

    public static class ValidString implements Validator {
        List<String> validStrings;

        private ValidString(List<String> validStrings) {
            this.validStrings = validStrings;
        }

        public static ValidString in(String... validStrings) {
            return new ValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (!validStrings.contains(s)) {
                throw new ConfigException(name, o, "String must be one of: " + Utils.join(validStrings, ", "));
            }

        }

        public String toString() {
            return "[" + Utils.join(validStrings, ", ") + "]";
        }
    }

    public static class ConfigKey {
        public final String name;
        public final Type type;
        public final String documentation;
        public final Object defaultValue;
        public final Validator validator;
        public final Importance importance;
        public final String group;
        public final int orderInGroup;
        public final Width width;
        public final String displayName;
        public final List<String> dependents;
        public final Recommender recommender;

        public ConfigKey(String name, Type type, Object defaultValue, Validator validator,
                         Importance importance, String documentation, String group,
                         int orderInGroup, Width width, String displayName,
                         List<String> dependents, Recommender recommender) {
            this.name = name;
            this.type = type;
            this.defaultValue = defaultValue;
            this.validator = validator;
            this.importance = importance;
            if (this.validator != null && this.hasDefault())
                this.validator.ensureValid(name, defaultValue);
            this.documentation = documentation;
            this.dependents = dependents;
            this.group = group;
            this.orderInGroup = orderInGroup;
            this.width = width;
            this.displayName = displayName;
            this.recommender = recommender;
        }

        public boolean hasDefault() {
            return this.defaultValue != NO_DEFAULT_VALUE;
        }
    }

    public String toHtmlTable() {
        List<ConfigKey> configs = sortedConfigs();
        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>\n");
        b.append("<th>Name</th>\n");
        b.append("<th>Description</th>\n");
        b.append("<th>Type</th>\n");
        b.append("<th>Default</th>\n");
        b.append("<th>Valid Values</th>\n");
        b.append("<th>Importance</th>\n");
        b.append("</tr>\n");
        for (ConfigKey def : configs) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append(def.name);
            b.append("</td>");
            b.append("<td>");
            b.append(def.documentation);
            b.append("</td>");
            b.append("<td>");
            b.append(def.type.toString().toLowerCase(Locale.ROOT));
            b.append("</td>");
            b.append("<td>");
            if (def.hasDefault()) {
                if (def.defaultValue == null)
                    b.append("null");
                else if (def.type == Type.STRING && def.defaultValue.toString().isEmpty())
                    b.append("\"\"");
                else
                    b.append(def.defaultValue);
            } else
                b.append("");
            b.append("</td>");
            b.append("<td>");
            b.append(def.validator != null ? def.validator.toString() : "");
            b.append("</td>");
            b.append("<td>");
            b.append(def.importance.toString().toLowerCase(Locale.ROOT));
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</tbody></table>");
        return b.toString();
    }

    /**
     * Get the configs formatted with reStructuredText, suitable for embedding in Sphinx
     * documentation.
     */
    public String toRst() {
        List<ConfigKey> configs = sortedConfigs();
        StringBuilder b = new StringBuilder();

        for (ConfigKey def : configs) {
            b.append("``");
            b.append(def.name);
            b.append("``\n");
            for (String docLine : def.documentation.split("\n")) {
                if (docLine.length() == 0) {
                    continue;
                }
                b.append("  ");
                b.append(docLine);
                b.append("\n\n");
            }
            b.append("  * Type: ");
            b.append(def.type.toString().toLowerCase(Locale.ROOT));
            b.append("\n");
            if (def.defaultValue != null) {
                b.append("  * Default: ");
                if (def.type == Type.STRING) {
                    b.append("\"");
                    b.append(def.defaultValue);
                    b.append("\"");
                } else {
                    b.append(def.defaultValue);
                }
                b.append("\n");
            }
            b.append("  * Importance: ");
            b.append(def.importance.toString().toLowerCase(Locale.ROOT));
            b.append("\n\n");
        }
        return b.toString();
    }

    /**
     * Get a list of configs sorted into "natural" order: listing required fields first, then
     * ordering by importance, and finally by name.
     */
    private List<ConfigKey> sortedConfigs() {
        // sort first required fields, then by importance, then name
        List<ConfigKey> configs = new ArrayList<>(this.configKeys.values());
        Collections.sort(configs, new Comparator<ConfigKey>() {
            public int compare(ConfigKey k1, ConfigKey k2) {
                // first take anything with no default value
                if (!k1.hasDefault() && k2.hasDefault()) {
                    return -1;
                } else if (!k2.hasDefault() && k1.hasDefault()) {
                    return 1;
                }

                // then sort by importance
                int cmp = k1.importance.compareTo(k2.importance);
                if (cmp == 0) {
                    // then sort in alphabetical order
                    return k1.name.compareTo(k2.name);
                } else {
                    return cmp;
                }
            }
        });
        return configs;
    }
}
