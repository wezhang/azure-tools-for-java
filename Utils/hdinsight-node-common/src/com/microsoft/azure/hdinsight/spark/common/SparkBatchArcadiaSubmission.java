package com.microsoft.azure.hdinsight.spark.common;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

import com.microsoft.azure.hdinsight.common.UriUtil;
import com.microsoft.azuretools.adauth.AuthException;
import com.microsoft.azuretools.adauth.PromptBehavior;
import com.microsoft.azuretools.authmanage.AdAuthManager;
import com.microsoft.azuretools.authmanage.AuthMethodManager;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.sdkmanage.AzureManager;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class SparkBatchArcadiaSubmission extends SparkBatchSubmission {
    public static final String ARCADIA_RESOURCE_ID = "5d13f7d7-0567-429c-9880-320e9555e5fc";

    private final @NotNull String workspaceName;
    private final @NotNull String tenantId;
    private final @NotNull URI livyUri;

    public SparkBatchArcadiaSubmission(final @NotNull String tenantId,
                                       final @NotNull String workspaceName,
                                       final @NotNull URI livyUri) {
        this.workspaceName = workspaceName;
        this.tenantId = tenantId;
        this.livyUri = UriUtil.normalizeWithSlashEnding(livyUri);
    }

    @Override
    public void setUsernamePasswordCredential(String username, String password) {
        throw new UnsupportedOperationException("Azure Arcadia does not support UserName/Password credential");
    }

    @NotNull
    private String getResourceEndpoint() {
        return ARCADIA_RESOURCE_ID;
    }

    @NotNull
    protected String getAccessToken() throws IOException {
        AzureManager azureManager = AuthMethodManager.getInstance().getAzureManager();
        // not signed in
        if (azureManager == null) {
            throw new AuthException("Not signed in. Can't send out the request.");
        }

        return AdAuthManager.getInstance().getAccessToken(getTenantId(), getResourceEndpoint(), PromptBehavior.Auto);
    }

    @NotNull
    public String getTenantId() {
        return tenantId;
    }

    @NotNull
    public String getWorkspaceName() {
        return workspaceName;
    }

    @NotNull
    @Override
    public CloseableHttpClient getHttpClient() throws IOException {
        return HttpClients.custom()
                .useSystemProperties()
                .setDefaultHeaders(Arrays.asList(
                        new BasicHeader("Authorization", "Bearer " + getAccessToken()),
                        new BasicHeader("x-ms-workspace-name", getWorkspaceName())))
                .setSSLSocketFactory(getSSLSocketFactory())
                .build();
    }

    @NotNull
    public URI getLivyUri() {
        return livyUri;
    }
}
