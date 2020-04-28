/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.agent.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.agent.action.Session;
import org.apache.ignite.internal.agent.dto.action.JobResponse;
import org.apache.ignite.internal.agent.dto.action.ResponseError;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.authentication.IgniteAuthenticationProcessor;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;

import static org.apache.ignite.internal.agent.dto.action.ResponseError.AUTHENTICATION_ERROR_CODE;
import static org.apache.ignite.internal.agent.dto.action.ResponseError.AUTHORIZE_ERROR_CODE;
import static org.apache.ignite.internal.agent.dto.action.ResponseError.INTERNAL_ERROR_CODE;
import static org.apache.ignite.internal.agent.dto.action.ResponseError.PARSE_ERROR_CODE;
import static org.apache.ignite.internal.agent.dto.action.Status.FAILED;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Utility methods.
 */
public final class AgentUtils {
    /** Agents path. */
    private static final String AGENTS_PATH = "/agents";

    /** */
    public static final String[] EMPTY = {};

    /**
     * Default constructor.
     */
    private AgentUtils() {
        // No-op.
    }

    /**
     * @param srvUri Server uri.
     * @param clusterId Cluster ID.
     */
    public static String monitoringUri(String srvUri, UUID clusterId) {
        return URI.create(srvUri + "/clusters/" + clusterId + "/monitoring-dashboard").normalize().toString();
    }

    /**
     * Prepare server uri.
     */
    public static URI toWsUri(String srvUri) {
        URI uri = URI.create(srvUri);

        if (uri.getScheme().startsWith("http")) {
            try {
                uri = new URI("http".equalsIgnoreCase(uri.getScheme()) ? "ws" : "wss",
                        uri.getUserInfo(),
                        uri.getHost(),
                        uri.getPort(),
                        AGENTS_PATH,
                        uri.getQuery(),
                        uri.getFragment()
                );
            }
            catch (URISyntaxException x) {
                throw new IllegalArgumentException(x.getMessage(), x);
            }
        }

        return uri;
    }

    /**
     * Authenticate by session and ignite security.
     *
     * @param security Security.
     * @param ses Session.
     */
    public static SecurityContext authenticate(
        IgniteSecurity security,
        Session ses
    ) throws IgniteAuthenticationException, IgniteCheckedException {
        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(ses.id());
        authCtx.nodeAttributes(Collections.emptyMap());
        authCtx.address(ses.address());
        authCtx.credentials(ses.credentials());

        SecurityContext subjCtx = security.authenticate(authCtx);

        if (subjCtx == null) {
            if (ses.credentials() == null) {
                throw new IgniteAuthenticationException(
                    "Failed to authenticate remote client (secure session SPI not set?): " + ses.id()
                );
            }

            throw new IgniteAuthenticationException(
                "Failed to authenticate remote client (invalid credentials?): " + ses.id()
            );
        }

        return subjCtx;
    }

    /**
     * Authenticate by session and authentication processor.
     *
     * @param authenticationProc Authentication processor.
     * @param ses Session.
     */
    public static AuthorizationContext authenticate(
        IgniteAuthenticationProcessor authenticationProc,
        Session ses
    ) throws IgniteCheckedException {
        SecurityCredentials creds = ses.credentials();

        String login = null;

        if (creds.getLogin() instanceof String)
            login = (String) creds.getLogin();

        String pwd = null;

        if (creds.getPassword() instanceof String)
            pwd = (String) creds.getPassword();

        return authenticationProc.authenticate(login, pwd);
    }

    /**
     * @param security Security.
     * @param perm Permission.
     */
    public static void authorizeIfNeeded(IgniteSecurity security, SecurityPermission perm) {
        authorizeIfNeeded(security, null, perm);
    }

    /**
     * @param security Security.
     * @param name Name.
     * @param perm Permission.
     */
    public static void authorizeIfNeeded(IgniteSecurity security, String name, SecurityPermission perm) {
        if (security.enabled())
            security.authorize(name, perm);
    }

    /**
     * @param ctx Context.
     * @param nodes Nodes.
     *
     * @return Set of supported cluster features.
     */
    public static Set<String> getClusterFeatures(GridKernalContext ctx, Collection<ClusterNode> nodes) {
        IgniteFeatures[] enums = IgniteFeatures.values();

        Set<String> features = U.newHashSet(enums.length);

        for (IgniteFeatures val : enums) {
            if (allNodesSupports(ctx, nodes, val))
                features.add(val.name());
        }

        return features;
    }

    /**
     * @param col Column.
     * @return Empty stream if collection is null else stream of collection elements.
     */
    public static <T> Stream<T> fromNullableCollection(Collection<T> col) {
        return col == null ? Stream.empty() : col.stream();
    }

    /**
     * Closes given processor ignoring possible checked exception.
     *
     * @param proc Processor.
     * @param log Logger.
     */
    public static void stopProcessor(GridProcessor proc, IgniteLogger log) {
        if (proc != null) {
            try {
                proc.stop(true);
            }
            catch (Exception e) {
                U.warn(log, "Failed to stop GridGain Control Center agent processor: " + proc.getClass(), e);
            }
        }
    }

    /**
     * Creates agent processor.
     *
     * @param log Logger.
     * @param clsName Processor class name.
     * @param paramTypes The parameter array.
     * @param args array of objects to be passed as arguments to the constructor call.
     * @return Agent processor.
     */
    public static <T extends GridProcessor> T createProcessor(IgniteLogger log, String clsName, Class<?>[] paramTypes, Object... args) {
        ClassLoader ldr = U.gridClassLoader();

        try {
            Class<?> mgrCls = ldr.loadClass(clsName);

            return (T)mgrCls.getConstructor(paramTypes).newInstance(args);
        }
        catch (Exception e) {
            U.error(log, "Failed to initialize GridGain Control Center agent processor: " + clsName, e);
        }

        return null;
    }

    /**
     * Start given processor ignoring possible checked exception.
     *
     * @param proc Processor.
     * @param log Logger.
     */
    public static void startProcessor(GridProcessor proc, IgniteLogger log) {
        if (proc != null) {
            try {
                proc.start();
            }
            catch (Exception e) {
                U.error(log, "Failed to start GridGain Control Center agent processor: " + proc.getClass(), e);
            }
        }
    }

    /**
     * @param id Id.
     * @param nodeConsistentId Node consistent id.
     * @param e Exception.
     */
    public static JobResponse convertToErrorJobResponse(UUID id, String nodeConsistentId, Throwable e) {
        return new JobResponse()
            .setRequestId(id)
            .setStatus(FAILED)
            .setError(new ResponseError(getErrorCode(e), e.getMessage(), e.getStackTrace()))
            .setNodeConsistentId(nodeConsistentId);
    }

    /**
     * @param e Exception.
     * @return Integer error code.
     */
    public static int getErrorCode(Throwable e) {
        if (e instanceof SecurityException || hasCause(e, SecurityException.class))
            return AUTHORIZE_ERROR_CODE;
        else if (e instanceof IgniteAuthenticationException ||
            e instanceof IgniteAccessControlException ||
            hasCause(e, IgniteAuthenticationException.class, IgniteAccessControlException.class)
        )
            return AUTHENTICATION_ERROR_CODE;
        else if (e instanceof IllegalArgumentException)
            return PARSE_ERROR_CODE;

        return INTERNAL_ERROR_CODE;
    }

    /**
     * @return Username of HTTP/HTTP proxy.
     */
    public static String getProxyUsername() {
        String httpsProxyUsername = System.getProperty("https.proxyUsername");

        return F.isEmpty(httpsProxyUsername) ? System.getProperty("http.proxyUsername") : httpsProxyUsername;
    }

    /**
     * @return Password of HTTP/HTTP proxy.
     */
    public static String getProxyPassword() {
        String httpsProxyPwd = System.getProperty("https.proxyPassword");

        return F.isEmpty(httpsProxyPwd) ? System.getProperty("http.proxyPassword") : httpsProxyPwd;
    }
}
