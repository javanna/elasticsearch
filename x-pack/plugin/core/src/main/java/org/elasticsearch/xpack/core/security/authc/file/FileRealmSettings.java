/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.file;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;

import java.util.Set;

public final class FileRealmSettings {
    public static final String TYPE = "file";

    private FileRealmSettings() {}

    /**
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting<?>> getSettings() {
        return CachingUsernamePasswordRealmSettings.getCachingSettings();
    }
}
