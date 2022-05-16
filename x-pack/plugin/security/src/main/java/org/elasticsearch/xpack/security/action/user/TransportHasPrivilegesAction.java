/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transport action that tests whether a user has the specified
 * {@link RoleDescriptor.IndicesPrivileges privileges}
 */
public class TransportHasPrivilegesAction extends HandledTransportAction<HasPrivilegesRequest, HasPrivilegesResponse> {

    private final AuthorizationService authorizationService;
    private final NativePrivilegeStore privilegeStore;
    private final SecurityContext securityContext;

    @Inject
    public TransportHasPrivilegesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        AuthorizationService authorizationService,
        NativePrivilegeStore privilegeStore,
        SecurityContext context
    ) {
        super(HasPrivilegesAction.NAME, transportService, actionFilters, HasPrivilegesRequest::new);
        this.authorizationService = authorizationService;
        this.privilegeStore = privilegeStore;
        this.securityContext = context;
    }

    @Override
    protected void doExecute(Task task, HasPrivilegesRequest request, ActionListener<HasPrivilegesResponse> listener) {
        final String username = request.username();
        final Subject subject = securityContext.getAuthentication().getEffectiveSubject();
        if (subject.getUser().principal().equals(username) == false) {
            listener.onFailure(new IllegalArgumentException("users may only check the privileges of their own account"));
            return;
        }

        final RoleDescriptor.IndicesPrivileges[] indicesPrivileges = request.indexPrivileges();
        if (indicesPrivileges != null) {
            for (int i = 0; i < indicesPrivileges.length; i++) {
                BytesReference query = indicesPrivileges[i].getQuery();
                if (query != null) {
                    listener.onFailure(
                        new IllegalArgumentException("users may only check the index privileges without any DLS role query")
                    );
                    return;
                }
            }
        }

        resolveApplicationPrivileges(
            request,
            ActionListener.wrap(
                applicationPrivilegeDescriptors -> authorizationService.checkPrivileges(
                    subject,
                    new AuthorizationEngine.PrivilegesToCheck(
                        request.clusterPrivileges(),
                        request.indexPrivileges(),
                        request.applicationPrivileges()
                    ),
                    applicationPrivilegeDescriptors,
                    listener.map(
                        privilegesCheckResult -> new HasPrivilegesResponse(
                            request.username(),
                            privilegesCheckResult.allMatch(),
                            privilegesCheckResult.cluster(),
                            privilegesCheckResult.index().values(),
                            privilegesCheckResult.application()
                        )
                    )
                ),
                listener::onFailure
            )
        );
    }

    private void resolveApplicationPrivileges(
        HasPrivilegesRequest request,
        ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener
    ) {
        final Set<String> applications = getApplicationNames(request);
        privilegeStore.getPrivileges(applications, null, listener);
    }

    public static Set<String> getApplicationNames(HasPrivilegesRequest request) {
        return Arrays.stream(request.applicationPrivileges())
            .map(RoleDescriptor.ApplicationResourcePrivileges::getApplication)
            .collect(Collectors.toSet());
    }
}
