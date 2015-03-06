/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ScriptModes {

    private static final String SCRIPT_SETTINGS_PREFIX = "script.";
    private static final String ENGINE_SETTINGS_PREFIX = "script.engine";

    final ImmutableMap<String, ScriptMode> scriptModes;

    //TODO document all the supported settings
    //TODO document that mustache scripts will be affected by generic settings, native won't
    //TODO document deprecation of script.disable_dynamic

    ScriptModes(Map<String, ScriptEngineService> scriptEngines, Settings settings) {

        //filter out the native engine as we don't want to apply fine grained settings to it.
        //native scripts are always on as they are static by definition.
        Map<String, ScriptEngineService> filteredEngines = Maps.newHashMap(scriptEngines);
        filteredEngines.remove(NativeScriptEngineService.NAME);

        Map<String, ScriptMode> scriptModesMap = Maps.newHashMap();

        //file scripts are enabled by default, for any language
        addGlobalScriptTypeModes(filteredEngines.keySet(), ScriptType.FILE, ScriptMode.ENABLE, scriptModesMap);
        //indexed scripts are enabled only for sandboxed languages by default
        addGlobalScriptTypeModes(filteredEngines.keySet(), ScriptType.INDEXED, ScriptMode.SANDBOX, scriptModesMap);
        //dynamic scripts are enabled only for sandboxed languages by default
        addGlobalScriptTypeModes(filteredEngines.keySet(), ScriptType.INLINE, ScriptMode.SANDBOX, scriptModesMap);

        for (ScriptType scriptType : ScriptType.values()) {
            String scriptTypeSetting = settings.get(SCRIPT_SETTINGS_PREFIX + scriptType);
            if (Strings.hasLength(scriptTypeSetting)) {
                ScriptMode scriptTypeMode = ScriptMode.parse(scriptTypeSetting);
                addGlobalScriptTypeModes(filteredEngines.keySet(), scriptType, scriptTypeMode, scriptModesMap);
            }
        }

        //operation based settings take precedence over script_type based settings, that's why they get expanded later
        for (ScriptedOp scriptedOp : ScriptedOp.values()) {
            ScriptMode scriptMode = extractScriptedOpMode(settings, SCRIPT_SETTINGS_PREFIX, scriptedOp);
            if (scriptMode != null) {
                addGlobalScriptedOpModes(filteredEngines.keySet(), scriptedOp, scriptMode, scriptModesMap);
            }
        }

        Map<String, Settings> langGroupedSettings = settings.getGroups(ENGINE_SETTINGS_PREFIX, true);
        for (Map.Entry<String, Settings> langSettings : langGroupedSettings.entrySet()) {
            //if settings refer to a non existing script lang, those settings will be ignored
            ScriptEngineService scriptEngineService = filteredEngines.get(langSettings.getKey());
            if (scriptEngineService != null) {
                for (ScriptType scriptType : ScriptType.values()) {
                    for (ScriptedOp scriptedOp : ScriptedOp.values()) {
                        ScriptMode scriptMode = extractScriptedOpMode(langSettings.getValue(), scriptType + ".", scriptedOp);
                        if (scriptMode != null) {
                            addScriptMode(scriptEngineService, scriptType, scriptedOp, scriptMode, scriptModesMap);
                        }
                    }
                }
            }
        }

        this.scriptModes = ImmutableMap.copyOf(scriptModesMap);
    }

    private static ScriptMode extractScriptedOpMode(Settings settings, String prefix, ScriptedOp scriptedOp) {
        String settingValue = settings.get(prefix + scriptedOp);
        if (Strings.hasLength(settingValue) == false) {
            for (String alternateName : scriptedOp.alternateNames()) {
                settingValue = settings.get(prefix + alternateName);
                if (Strings.hasLength(settingValue)) {
                    break;
                }
            }
        }
        if (Strings.hasLength(settingValue)) {
            return ScriptMode.parse(settingValue);
        }
        return null;
    }

    private static void addGlobalScriptTypeModes(Set<String> langs, ScriptType scriptType,
                                                 ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        for (String lang : langs) {
            for (ScriptedOp scriptedOp : ScriptedOp.values()) {
                addScriptMode(lang, scriptType, scriptedOp, scriptMode, scriptModes);
            }
        }
    }

    private static void addGlobalScriptedOpModes(Set<String> langs, ScriptedOp scriptedOp,
                                                 ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        for (String lang : langs) {
            for (ScriptType scriptType : ScriptType.values()) {
                addScriptMode(lang, scriptType, scriptedOp, scriptMode, scriptModes);
            }
        }
    }

    private static void addScriptMode(ScriptEngineService scriptEngineService, ScriptType scriptType, ScriptedOp scriptedOp, ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        //expand the lang specific settings to all of the different names given to each scripting language
        for (String scriptEngineName : scriptEngineService.types()) {
            addScriptMode(scriptEngineName, scriptType, scriptedOp, scriptMode, scriptModes);
        }
    }

    private static void addScriptMode(String lang, ScriptType scriptType, ScriptedOp scriptedOp, ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        scriptModes.put(ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + scriptedOp, scriptMode);
    }

    /**
     * Returns the script mode for a script of a certain written in a certain language,
     * of a certain type and executing as part of a specific operation/api.
     *
     * @param lang the language that the script is written in
     * @param scriptType the type of the script
     * @param scriptedOp the api that requires the execution of the script
     * @return whether the script is enabled, disabled, or only enabled for sandboxed languages
     */
    public ScriptMode getScriptMode(String lang, ScriptType scriptType, ScriptedOp scriptedOp) {
        ScriptMode scriptMode = scriptModes.get(ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + scriptedOp);
        if (scriptMode == null) {
            throw new ElasticsearchIllegalArgumentException("script mode not found for lang [" + lang + "], script_type [" + scriptType + "], operatio [" + scriptedOp + "]");
        }
        return scriptMode;
    }

    @Override
    public String toString() {
        //order settings by key before printing them out, for readability
        TreeMap<String, ScriptMode> scriptModesTreeMap = new TreeMap<>();
        scriptModesTreeMap.putAll(scriptModes);
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, ScriptMode> stringScriptModeEntry : scriptModesTreeMap.entrySet()) {
            stringBuilder.append(stringScriptModeEntry.getKey()).append(": ").append(stringScriptModeEntry.getValue()).append("\n");
        }
        return stringBuilder.toString();
    }
}
