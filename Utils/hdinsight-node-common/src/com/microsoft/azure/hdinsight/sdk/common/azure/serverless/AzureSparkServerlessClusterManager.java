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
import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.hdinsight.common.logger.ILogger;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.sdk.cluster.ServerlessClusterManager;
import com.microsoft.azure.hdinsight.sdk.common.AzureHttpObservable;
import com.microsoft.azuretools.authmanage.AuthMethodManager;
import com.microsoft.azuretools.authmanage.CommonSettings;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import com.microsoft.azuretools.sdkmanage.AzureManager;
import rx.Observable;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AzureSparkServerlessClusterManager implements ServerlessClusterManager,
                                                           ILogger {
    // Lazy singleton initialization
    private static class LazyHolder {
        static final AzureSparkServerlessClusterManager INSTANCE =
                new AzureSparkServerlessClusterManager();
    }
    public static ServerlessClusterManager getInstance() {
        return LazyHolder.INSTANCE;
    }

    //
    // Fields
    //
    private static final String REST_SEGMENT_SUBSCRIPTION = "/subscriptions/";
    private static final String REST_SEGMENT_ADL_ACCOUNT = "providers/Microsoft.DataLakeAnalytics/accounts";

    @NotNull
    private final AzureHttpObservable http;

    @NotNull
    private AzureEnvironment azureEnv = CommonSettings.getAdEnvironment();

    @NotNull
    private ImmutableSortedSet<? extends AzureSparkServerlessAccount> accounts= ImmutableSortedSet.of();

    public AzureSparkServerlessClusterManager() {
        // TODO: Use ApiVersion.VERSION to replace the api-version string
        this.http = new AzureHttpObservable("2016-11-01");
    }

    //
    // Getters / setters
    //

    @NotNull
    public AzureHttpObservable getHttp() {
        return http;
    }

    @NotNull
    public ImmutableSortedSet<? extends AzureSparkServerlessAccount> getAccounts() {
        return accounts;
    }

    @Nullable
    public AzureManager getAzureManager() {
        try {
            return AuthMethodManager.getInstance().getAzureManager();
        } catch (IOException e) {
            log().info("Can't get Azure manager now. error: " + e);

            return null;
        }
    }

    @NotNull
    @Override
    public ImmutableSortedSet<? extends IClusterDetail> getClusters() {
        if (getAzureManager() == null) {
            return ImmutableSortedSet.of();
        }

        return ImmutableSortedSet.copyOf(accounts.stream()
                .flatMap(account -> account.getClusters().stream())
                .iterator());
    }

    @NotNull
    @Override
    public ServerlessClusterManager refresh() {
        if (getAzureManager() == null) {
            return this;
        }

        return get().toBlocking().singleOrDefault(this);
    }

    public Observable<AzureSparkServerlessClusterManager> get() {
        return getAzureDataLakeAccountsRequest()
                .map(this::updateWithResponse)
                .defaultIfEmpty(this);
    }

    private Observable<Set<Object>> getAzureDataLakeAccountsRequest() {
        if (getAzureManager() == null) {
            return Observable.empty();
        }

        // Loop subscriptions to get all accounts
        return Observable
                .fromCallable(() -> getAzureManager().getSubscriptions()
                        .stream()
                        .map(subscription -> getSubscriptionsUri(subscription.subscriptionId())
                                .resolve(REST_SEGMENT_ADL_ACCOUNT))
                        .collect(Collectors.toSet()))  // Output ADL account URI list
                .flatMap(Observable::from)             // Send URI to next one by one
                .map(uri -> getHttp()
                        .withUuidUserAgent(false)
                        // TODO:: handle Azure DataLake account list request, map it to account
                        .get(uri.toString(), null, null, Object.class))
                .toList()
                .map(adlAccountsSet -> adlAccountsSet.stream()
                        .flatMap(adlAccounts -> {
                            try {
                                // May produce NoElementException
                                return Stream.of(adlAccounts.toBlocking().single());
                            } catch (Exception e) {
                                log().warn("Can't get Azure Spark Serverless Account " + e);
                                return Stream.empty();
                            }
                        })
                        .collect(Collectors.toSet()));
    }

    @NotNull
    private URI getSubscriptionsUri(@NotNull String subscriptionId) {
        return URI.create(azureEnv.managementEndpoint())
                .resolve(REST_SEGMENT_SUBSCRIPTION)
                .resolve(subscriptionId);
    }

    @NotNull
    private AzureSparkServerlessClusterManager updateWithResponse(Set<Object> accountsResponse) {
        // TODO: update self with accounts response
        return this;
    }

}
