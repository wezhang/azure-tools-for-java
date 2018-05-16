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
import com.microsoft.azure.hdinsight.sdk.cluster.DestroyableCluster;
import com.microsoft.azure.hdinsight.sdk.cluster.ProvisionableCluster;
import com.microsoft.azure.hdinsight.sdk.cluster.SparkCluster;
import com.microsoft.azure.hdinsight.sdk.common.AzureHttpObservable;
import com.microsoft.azure.hdinsight.sdk.common.HDIException;
import com.microsoft.azure.hdinsight.sdk.common.HttpResponse;
import com.microsoft.azure.hdinsight.sdk.rest.azure.serverless.spark.models.*;
import com.microsoft.azure.hdinsight.sdk.storage.HDStorageAccount;
import com.microsoft.azure.hdinsight.sdk.storage.IHDIStorageAccount;
import com.microsoft.azuretools.authmanage.models.SubscriptionDetail;
import com.microsoft.azuretools.azurecommons.helpers.AzureCmdException;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import org.apache.http.entity.StringEntity;
import rx.Observable;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class AzureSparkServerlessCluster extends SparkCluster
                                         implements ProvisionableCluster,
                                                    ServerlessCluster,
        DestroyableCluster {
    public static class SparkResource {
        int instances;
        int coresPerInstance;
        int memoryGBSizePerInstance;

        SparkResource setInstances(int instances) {
            this.instances = instances;

            return this;
        }

        SparkResource setCoresPerInstance(int coresPerInstance) {
            this.coresPerInstance = coresPerInstance;

            return this;
        }

        SparkResource setMemoryGBSizePerInstance(int memoryGBSizePerInstance) {
            this.memoryGBSizePerInstance = memoryGBSizePerInstance;

            return this;
        }
    }

    public static class Builder {
        @NotNull
        private final SubscriptionDetail subscription;
        @NotNull
        private final URI accountUri;
        @NotNull
        private String name = "unnamed";
        @NotNull
        private String resourcePoolVersion = "";
        @NotNull
        private String sparkVersion = "2.3.1";
        @NotNull
        private String userStorageAccount = "";
        @NotNull
        private String sparkEventsPath = "";
        private int masterInstances = 1;
        private int masterPerInstanceCores = 2;
        private int masterPerInstanceMemory = 16;
        private int workerInstances = 2;
        private int workerPerInstanceCores = 2;
        private int workerPerInstanceMemory = 16;

        public Builder(@NotNull SubscriptionDetail subscription, @NotNull URI accountUri) {
            this.subscription = subscription;
            this.accountUri = accountUri;
        }


        public Builder name(@NotNull String name) {
            this.name = name;

            return this;
        }

        public Builder resourcePoolVersion(@NotNull String rpVersion) {
            this.resourcePoolVersion = rpVersion;

            return this;
        }

        public Builder sparkVersion(@NotNull String sparkVersion) {
            this.sparkVersion = sparkVersion;

            return this;
        }

        public Builder userStorageAccount(@NotNull String userStorageAccount) {
            this.userStorageAccount = userStorageAccount;

            return this;
        }

        public Builder sparkEventsPath(@NotNull String eventsPath) {
            this.sparkEventsPath = eventsPath;

            return this;
        }

        public Builder masterInstances(int instances) {
            this.masterInstances = instances;

            return this;
        }

        public Builder masterPerInstanceCores(int cores) {
            this.masterPerInstanceCores = cores;

            return this;
        }

        public Builder masterPerInstanceMemory(int sizeInGB) {
            this.masterPerInstanceMemory = sizeInGB;

            return this;
        }

        public Builder workerInstances(int instances) {
            this.workerInstances = instances;

            return this;
        }

        public Builder workerPerInstanceCores(int cores) {
            this.workerPerInstanceCores = cores;

            return this;
        }

        public Builder workerPerInstanceMemory(int sizeInGB) {
            this.workerPerInstanceMemory = sizeInGB;

            return this;
        }

        public AzureSparkServerlessCluster build() {
            AzureSparkServerlessCluster cluster = new AzureSparkServerlessCluster(
                    this.subscription, this.accountUri, UUID.randomUUID().toString());

            cluster.name = this.name;
            cluster.resourcePoolVersion = this.resourcePoolVersion;
            cluster.sparkVersion = this.sparkVersion;
            cluster.userStorageAccount = this.userStorageAccount;
            cluster.sparkEventsPath = this.sparkEventsPath;
            cluster.state = "unprovisioned";

            cluster.master = new SparkResource()
                    .setInstances(this.masterInstances)
                    .setCoresPerInstance(this.masterPerInstanceCores)
                    .setMemoryGBSizePerInstance(this.masterPerInstanceMemory);

            cluster.worker = new SparkResource()
                    .setInstances(this.workerInstances)
                    .setCoresPerInstance(this.workerPerInstanceCores)
                    .setMemoryGBSizePerInstance(this.workerPerInstanceMemory);

            return cluster;
        }
    }

    private static final String REST_SEGMENT = "activityTypes/spark/resourcePools/";

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

    @Nullable
    private String name;
    @Nullable
    private String sparkVersion;
    @Nullable
    private String state;
    @Nullable
    private String connectionUrl;
    @Nullable
    private String createDate;

    @NotNull
    private String resourcePoolVersion = "";
    @NotNull
    private String userStorageAccount = "";
    @NotNull
    private String sparkEventsPath = "";

    @Nullable
    private SparkResource master;
    @Nullable
    private SparkResource worker;

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

    @Nullable
    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getTitle() {
        return name + "[" + (state != null ? state.toUpperCase() : "UNKNOWN") + "]";
    }

    @Nullable
    @Override
    public String getState() {
        return state;
    }

    @Override
    public String getLocation() {
        return null;
    }

    @Nullable
    @Override
    public String getConnectionUrl() {
        return connectionUrl;
    }

    @Nullable
    @Override
    public String getCreateDate() {
        return createDate;
    }

    @Override
    public String getVersion() {
        return resourcePoolVersion;
    }

    @NotNull
    @Override
    public SubscriptionDetail getSubscription() {
        return subscription;
    }

    @Override
    public int getDataNodes() {
        return 0;
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
    public String getOSType() {
        return "windows";
    }

    @Override
    public String getResourceGroup() {
        return null;
    }

    @Nullable
    @Override
    public IHDIStorageAccount getStorageAccount() throws HDIException {
        return null;
    }

    @Override
    public List<HDStorageAccount> getAdditionalStorageAccounts() {
        return null;
    }

    @Override
    public synchronized void getConfigurationInfo() throws IOException, HDIException, AzureCmdException {
        // Get the information from the server, block.
        isConfigInfoAvailable = false;

        get().toBlocking().subscribe();

        isConfigInfoAvailable = true;
    }

    @Nullable
    @Override
    public String getSparkVersion() {
        return sparkVersion;
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

    private Observable<SparkResourcePool> getResourcePoolRequest() {
        URI uri = getUri();

        return getHttp()
                .setUserAgent(getUserAgent(false))
                .get(uri.toString(), null, null, SparkResourcePool.class);
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

    private AzureSparkServerlessCluster updateWithResponse(@NotNull SparkResourcePool resourcePoolResp) {
        this.state = resourcePoolResp.state().toString();
        this.createDate = resourcePoolResp.startTime().toString();

        // FIXME!!! sparkUriCollection field is missing
        // set connectionUrl

        return this;
    }

    @NotNull
    @Override
    public Observable<? extends ProvisionableCluster> provision() {
        return createResourcePoolRequest()
                .map(this::updateWithResponse)
                .defaultIfEmpty(this);
    }

    private Observable<SparkResourcePool> createResourcePoolRequest() {
        URI uri = getUri();

        CreateSparkResourcePool putBody = preparePutResourcePool();
        if (putBody == null) {
            return Observable.error(new IllegalAccessException("The Spark resource pool parameters are not setup well"));
        }

        String json = putBody.convertToJson()
                .orElseThrow(() -> new IllegalArgumentException("Bad Spark resource pool arguments to put"));

        StringEntity entity = new StringEntity(json, StandardCharsets.UTF_8);
        entity.setContentType("application/json");

        return getHttp()
                .setUserAgent(getUserAgent(true))
                // FIXME!! Need to confirm the response type PutResourcePoolIdResponse or SparkResourcePool
                .put(uri.toString(), entity, null, null, SparkResourcePool.class);
    }

    @Nullable
    private CreateSparkResourcePool preparePutResourcePool() {
        if (master == null || worker == null) {
            return null;
        }

        CreateSparkResourcePool putBody = new CreateSparkResourcePool();

        putBody.withName(getName())
               .withProperties(new CreateSparkResourcePoolParameters()
                       .withResourcePoolVersion(this.resourcePoolVersion)
                       .withSparkVersion(this.sparkVersion)
                       // FIXME!! UserStorageAccount is missing
                       .withSparkEventsDirectoryPath(this.sparkEventsPath)
                       .withSparkResourceCollection(Arrays.asList(
                               new CreateSparkResourcePoolItemParameters()
                                       .withName(SparkNodeType.SPARK_MASTER)
                                       .withTargetInstanceCount(master.instances)
                                       .withPerInstanceCoreCount(master.coresPerInstance)
                                       .withPerInstanceMemoryInGB(master.memoryGBSizePerInstance),
                               new CreateSparkResourcePoolItemParameters()
                                       .withName(SparkNodeType.SPARK_WORKER)
                                       .withTargetInstanceCount(worker.instances)
                                       .withPerInstanceCoreCount(worker.coresPerInstance)
                                       .withPerInstanceMemoryInGB(worker.memoryGBSizePerInstance)
                       )));

        return putBody;
    }

    @NotNull
    @Override
    public Observable<? extends DestroyableCluster> destroy() {
        return deleteResourcePoolRequest()
                .map(resp -> {
                    // FIXME!! Replace the string with Enum
                    this.state = "deleted";

                    return this;
                })
                .defaultIfEmpty(this);
    }

    private Observable<HttpResponse> deleteResourcePoolRequest() {
        URI uri = getUri();

        return getHttp()
                .setUserAgent(getUserAgent(true))
                .delete(uri.toString(), null, null);
    }
}
