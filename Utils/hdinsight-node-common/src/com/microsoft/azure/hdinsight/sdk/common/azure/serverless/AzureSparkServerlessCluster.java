/*
 * Copyright (c) Microsoft Corporation
 *
 * All rights reserved.
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 * THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.hdinsight.sdk.common.azure.serverless;

import com.microsoft.azure.hdinsight.common.HDInsightLoader;
import com.microsoft.azure.hdinsight.common.appinsight.AppInsightsHttpRequestInstallIdMapRecord;
import com.microsoft.azure.hdinsight.sdk.cluster.DestoryableCluster;
import com.microsoft.azure.hdinsight.sdk.cluster.ProvisionableCluster;
import com.microsoft.azure.hdinsight.sdk.cluster.SparkCluster;
import com.microsoft.azure.hdinsight.sdk.common.AzureHttpObservable;
import com.microsoft.azure.hdinsight.sdk.common.HDIException;
import com.microsoft.azuretools.authmanage.models.SubscriptionDetail;
import com.microsoft.azuretools.azurecommons.helpers.AzureCmdException;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import org.apache.commons.lang3.NotImplementedException;
import rx.Observable;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

public class AzureSparkServerlessCluster extends SparkCluster
                                         implements ProvisionableCluster,
                                                    ServerlessCluster,
                                                    DestoryableCluster {
    public static final String REST_SEGMENT = "activityTypes/spark/resourcePools/";

    @NotNull
    private final SubscriptionDetail subscription;

    @NotNull
    private final AzureHttpObservable http;

    @NotNull
    private final String apiVersion = "2018-02-01-preview"; // Preview version

    @NotNull
    private final URI accountUri;

    @NotNull
    private final String guid;

    private boolean isConfigInfoAvailable;

    public AzureSparkServerlessCluster(@NotNull SubscriptionDetail subscription, @NotNull URI accountUri, @NotNull String guid) {
        this.subscription = subscription;
        this.http = new AzureHttpObservable(subscription, this.apiVersion);
        this.accountUri = accountUri;
        this.guid = guid;
    }

    @Override
    public boolean isEmulator() {
        return false;
    }

    @Override
    public boolean isConfigInfoAvailable() {
        return isConfigInfoAvailable;
    }

    @NotNull
    @Override
    public SubscriptionDetail getSubscription() {
        return subscription;
    }

    @Override
    public String getHttpUserName() throws HDIException {
        throw new HDIException("Azure Spark Serverless Cluster doesn't support HTTP username/password certificate");
    }

    @Override
    public String getHttpPassword() throws HDIException {
        throw new HDIException("Azure Spark Serverless Cluster doesn't support HTTP username/password certificate");
    }

    @Override
    public synchronized void getConfigurationInfo() throws IOException, HDIException, AzureCmdException {
        // Get the information from the server, block.
        isConfigInfoAvailable = false;

        get().toBlocking().subscribe();

        isConfigInfoAvailable = true;
    }

    @NotNull
    public URI getAccountUri() {
        return accountUri;
    }

    @NotNull
    public AzureHttpObservable getHttp() {
        return http;
    }

    public Observable<AzureSparkServerlessCluster> get() {
        return getResourcePoolRequest()
                .map(this::updateWithResponse)
                .defaultIfEmpty(this);
    }

    private Observable<Object> getResourcePoolRequest() {
        URI uri = getUri();

        return getHttp()
                .setUserAgent(getUserAgent(false))
                .get(uri.toString(), null, null, Object.class); // FIXME!!! replace Object.class with response type.
    }

    @NotNull
    public URI getUri() {
        return getAccountUri().resolve(REST_SEGMENT + guid);
    }

    @Nullable
    public String getUserAgent(boolean isMapToInstallID) {
        String originUa = getHttp().getUserAgent();

        if (originUa == null) {
            return null;
        }

        String requestId = UUID.randomUUID().toString();

        if (isMapToInstallID) {
            new AppInsightsHttpRequestInstallIdMapRecord(requestId, getInstallationID()).post();
        }

        return String.format("%s %s", originUa.trim(), requestId);
    }

    @NotNull
    private String getInstallationID() {
        if (HDInsightLoader.getHDInsightHelper() == null) {
            return "";
        }

        return HDInsightLoader.getHDInsightHelper().getInstallationId();
    }

    private AzureSparkServerlessCluster updateWithResponse(@NotNull Object resourcePoolResp) {
        // TODO: Handle response
        return this;
    }

    @NotNull
    @Override
    public Observable<DestoryableCluster> destory() {
        // TODO:
        throw new NotImplementedException("Destory a cluster isn't implemented");
    }

    @NotNull
    @Override
    public Observable<ProvisionableCluster> provision() {
        // TODO:
        throw new NotImplementedException("Provision a cluster isn't implemented");
    }
}
