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
import com.microsoft.azure.hdinsight.sdk.cluster.ClusterContainer;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.sdk.common.AzureHttpObservable;
import com.microsoft.azure.hdinsight.sdk.common.AzureManagementHttpObservable;
import com.microsoft.azure.hdinsight.sdk.common.ODataParam;
import com.microsoft.azure.hdinsight.sdk.rest.azure.datalake.analytics.accounts.models.api.GetAccountsListResponse;
import com.microsoft.azure.hdinsight.sdk.rest.azure.datalake.analytics.accounts.models.ApiVersion;
import com.microsoft.azure.hdinsight.sdk.rest.azure.datalake.analytics.accounts.models.DataLakeAnalyticsAccount;
import com.microsoft.azure.hdinsight.sdk.rest.azure.datalake.analytics.accounts.models.DataLakeAnalyticsAccountBasic;
import com.microsoft.azuretools.adauth.AuthException;
import com.microsoft.azuretools.authmanage.AuthMethodManager;
import com.microsoft.azuretools.authmanage.CommonSettings;
import com.microsoft.azuretools.authmanage.models.SubscriptionDetail;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import com.microsoft.azuretools.sdkmanage.AzureManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static rx.Observable.concat;
import static rx.Observable.from;

public class AzureSparkServerlessClusterManager implements ClusterContainer,
                                                           ILogger {
    // Lazy singleton initialization
    private static class LazyHolder {
        static final AzureSparkServerlessClusterManager INSTANCE =
                new AzureSparkServerlessClusterManager();
    }
    public static AzureSparkServerlessClusterManager getInstance() {
        return LazyHolder.INSTANCE;
    }

    //
    // Fields
    //
    private static final String REST_SEGMENT_SUBSCRIPTION = "/subscriptions/";
    private static final String REST_SEGMENT_ADL_ACCOUNT = "providers/Microsoft.DataLakeAnalytics/accounts";

    // FIXME!!!
    private static final String ACCOUNT_FILTER = CommonSettings.getAdEnvironment().endpoints()
            .getOrDefault("dataLakeSparkAccountFilter",
                    "length(name) gt 4 and substring(name, length(name) sub 4) ge '-c00' and substring(name, length(name) sub 4) le '-c99'");

    @NotNull
    private final HashMap<String, AzureHttpObservable> httpMap = new HashMap<>();

    @NotNull
    private AzureEnvironment azureEnv = CommonSettings.getAdEnvironment();

    @NotNull
    private ImmutableSortedSet<? extends AzureSparkServerlessAccount> accounts= ImmutableSortedSet.of();

    public AzureSparkServerlessClusterManager() {
        this.httpMap.put("common", new AzureHttpObservable(ApiVersion.VERSION));

        AuthMethodManager.getInstance().addSignOutEventListener(() -> accounts = ImmutableSortedSet.of());
    }

    //
    // Getters / setters
    //

    @NotNull
    public HashMap<String, AzureHttpObservable> getHttpMap() {
        return httpMap;
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
    public ClusterContainer refresh() {
        if (getAzureManager() == null) {
            return this;
        }

        try {
            return get().toBlocking().singleOrDefault(this);
        } catch (Exception ex) {
            log().warn("Got exceptions when refresh Azure Data Lake Spark pool: " + ex);

            return this;
        }
    }

    public Observable<AzureSparkServerlessClusterManager> get() {
        return getAzureDataLakeAccountsRequest()
                .map(this::updateWithResponse)
                .defaultIfEmpty(this);
    }

    public Observable<AzureSparkServerlessClusterManager> fetchClusters() {
        return get()
                .map(AzureSparkServerlessClusterManager::getAccounts)
                .flatMap(Observable::from)
                .observeOn(Schedulers.io())
                .flatMap(AzureSparkServerlessAccount::get)
                .map(account -> account.getClusters())
                .flatMap(Observable::from)
                .flatMap(cluster -> ((AzureSparkServerlessCluster)cluster).get())
                .toSortedList()
                .map(clusters -> this)
                .defaultIfEmpty(this);
    }

    private Observable<List<Triple<SubscriptionDetail, DataLakeAnalyticsAccountBasic, DataLakeAnalyticsAccount>>>
    getAzureDataLakeAccountsRequest() {
        if (getAzureManager() == null) {
            return Observable.error(new AuthException("Can't get Azure Data Lake account since the user isn't signed in, please sign in by Azure Explorer."));
        }

        // Loop subscriptions to get all accounts
        return Observable
                .fromCallable(() -> getAzureManager().getSubscriptionManager().getSubscriptionDetails())
                .flatMap(Observable::from)             // Get Subscription details one by one
                .map(sub -> Pair.of(
                        sub,
                        URI.create(getSubscriptionsUri(sub.getSubscriptionId()).toString() + "/")
                           .resolve(REST_SEGMENT_ADL_ACCOUNT)))
                .doOnNext(pair -> log().debug("Pair(Subscription, AccountsListUri): " + pair.toString()))
                .map(subUriPair -> Pair.of(
                        subUriPair.getLeft(),
                        getHttp(subUriPair.getLeft())
                                .withUuidUserAgent()
                                .get(subUriPair.getRight().toString(),
                                        Collections.singletonList(ODataParam.filter(ACCOUNT_FILTER)),
                                        null,
                                        // FIXME!!! Needs to support paging
                                        GetAccountsListResponse.class)))
                // account basic list -> account basic
                .flatMap(subAccountsObPair -> subAccountsObPair.getRight()
                                .flatMap(accountsResp -> Observable.from(accountsResp.items()))
                                .subscribeOn(Schedulers.io())
                                .map(accountBasic -> Pair.of(subAccountsObPair.getLeft(), accountBasic)))
                .flatMap(subAccountBasicPair -> {
                    // accountBasic.id is the account detail absolute URI path
                    URI accountDetailUri = getResourceManagerEndpoint().resolve(subAccountBasicPair.getRight().id());

                    // Get account details
                    return getHttp(subAccountBasicPair.getLeft())
                            .withUuidUserAgent()
                            .get(accountDetailUri.toString(), null, null, DataLakeAnalyticsAccount.class)
                            .map(accountDetail -> Triple.of(
                                    subAccountBasicPair.getLeft(), subAccountBasicPair.getRight(), accountDetail));
                })
                .toList()
                .doOnNext(triples -> log().debug("Triple(Subscription, AccountBasic, AccountDetails) list: " + triples.toString()));
    }

    @NotNull
    private synchronized AzureHttpObservable getHttp(SubscriptionDetail subscriptionDetail) {
        if (httpMap.containsKey(subscriptionDetail.getSubscriptionId())) {
            return httpMap.get(subscriptionDetail.getSubscriptionId());
        }

        AzureHttpObservable subHttp = new AzureManagementHttpObservable(subscriptionDetail, ApiVersion.VERSION);
        httpMap.put(subscriptionDetail.getSubscriptionId(), subHttp);

        return subHttp;
    }

    @NotNull
    private URI getResourceManagerEndpoint() {
        return URI.create(azureEnv.resourceManagerEndpoint());
    }

    @NotNull
    private URI getSubscriptionsUri(@NotNull String subscriptionId) {
        return getResourceManagerEndpoint()
                .resolve(REST_SEGMENT_SUBSCRIPTION)
                .resolve(subscriptionId);
    }

    @NotNull
    private AzureSparkServerlessClusterManager updateWithResponse(
            List<Triple<SubscriptionDetail, DataLakeAnalyticsAccountBasic, DataLakeAnalyticsAccount>> accountsResponse) {
        accounts = ImmutableSortedSet.copyOf(accountsResponse
                .stream()
                .map(subAccountBasicDetailTriple ->     // Triple: subscription, accountBasic, accountDetail
                        new AzureSparkServerlessAccount(
                                subAccountBasicDetailTriple.getLeft(),
                                // endpoint property is account's base URI
                                URI.create("https://" + subAccountBasicDetailTriple.getMiddle().endpoint()),
                                           subAccountBasicDetailTriple.getMiddle().name())
                                   .setBasicResponse(subAccountBasicDetailTriple.getMiddle())
                                   .setDetailResponse(subAccountBasicDetailTriple.getRight()))
                .iterator());

        return this;
    }

    public Observable<? extends AzureSparkServerlessCluster> findCluster(@NotNull String accountName, @NotNull String clusterGuid) {
        return concat(from(getAccounts()), get().flatMap(manager -> from(manager.getAccounts())))
                .filter(account -> account.getName().equals(accountName))
                .first()
                .flatMap(account -> concat(from(account.getClusters()), account.get().flatMap(acct -> from(acct.getClusters()))))
                .map(AzureSparkServerlessCluster.class::cast)
                .filter(cluster -> cluster.getGuid().equals(clusterGuid))
                .first();
    }

    public Observable<Boolean> isFeatureEnabled() {
        return concat(from(getAccounts()), get().flatMap(manager -> from(manager.getAccounts())))
                .isEmpty()
                .map(isEmpty -> !isEmpty)
                .onErrorReturn(err -> {
                    log().warn("Checking Azure Data Lake Spark pool got error: " + err);

                    return false;
                });
    }
}
