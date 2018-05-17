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

import com.google.common.collect.ImmutableSortedSet;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.sdk.common.AzureHttpObservable;
import com.microsoft.azure.hdinsight.sdk.rest.azure.serverless.spark.models.ApiVersion;
import com.microsoft.azure.hdinsight.sdk.rest.azure.serverless.spark.models.SparkResourcePoolList;
import com.microsoft.azuretools.authmanage.models.SubscriptionDetail;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import jdk.nashorn.internal.objects.NativeArray;
import org.apache.commons.lang3.StringUtils;
import rx.Observable;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class AzureSparkServerlessAccount {
    private static final String REST_SEGMENT_SPARK_RESOURCEPOOLS = "activityTypes/spark/resourcePools";

    @NotNull
    private final SubscriptionDetail subscription;

    @NotNull
    private final AzureHttpObservable http;

    @NotNull
    private final String apiVersion = ApiVersion.VERSION;

    @NotNull
    private final URI uri;

    @Nullable
    private String id;

    @NotNull
    private ImmutableSortedSet<? extends IClusterDetail> clusters = ImmutableSortedSet.of();

    public AzureSparkServerlessAccount(@NotNull SubscriptionDetail subscription, @NotNull URI uri) {
        this.subscription = subscription;
        this.http = new AzureHttpObservable(subscription, this.apiVersion);
        this.uri = uri;
    }

    //
    // Getters / setters
    //

    @NotNull
    public SubscriptionDetail getSubscription() {
        return subscription;
    }

    @NotNull
    public AzureHttpObservable getHttp() {
        return http;
    }

    @NotNull
    public URI getUri() {
        return uri;
    }

    //
    // RestFUL API operations
    //

    @NotNull
    public Observable<AzureSparkServerlessAccount> get() {
        return getResourcePoolsRequest()
                .map(this::updateWithResponse)
                .defaultIfEmpty(this);
    }

    private Observable<SparkResourcePoolList> getResourcePoolsRequest() {
        URI uri = getUri();

        return getHttp()
                .withUuidUserAgent(false)
                .get(uri.toString(), null, null, SparkResourcePoolList.class);
    }

    private AzureSparkServerlessAccount updateWithResponse(SparkResourcePoolList sparkResourcePoolList) {
        clusters = ImmutableSortedSet.copyOf(sparkResourcePoolList.value().stream()
                .map(analyticsActivity -> new AzureSparkServerlessCluster(this, analyticsActivity.id().toString())
                        .updateWithAnalyticsActivity(analyticsActivity))
                .iterator());

        return this;
    }

    @NotNull
    public ImmutableSortedSet<? extends IClusterDetail> getClusters() {
        return clusters;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        return obj != null && StringUtils.equalsIgnoreCase(id, ((AzureSparkServerlessAccount) obj).id);
    }
}
