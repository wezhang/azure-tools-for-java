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

package com.microsoft.azure.hdinsight.spark.common;

import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azure.hdinsight.sdk.common.HttpResponse;
import com.microsoft.azure.hdinsight.sdk.common.azure.serverless.AzureSparkServerlessCluster;
import com.microsoft.azure.hdinsight.sdk.common.azure.serverless.AzureSparkServerlessClusterManager;
import com.microsoft.azure.hdinsight.spark.jobs.JobUtils;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.jsoup.Jsoup;
import rx.Observable;
import rx.Observer;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static rx.exceptions.Exceptions.propagate;

public class ServerlessSparkBatchJob extends SparkBatchJob {
    private final String STANDALONE_SPARK_APPID_REGEX = "(app-[-\\d]+)";
    private final Pattern appIdPattern = Pattern.compile(STANDALONE_SPARK_APPID_REGEX);
    private final Pattern totalUptimeIdPattern = Pattern.compile("Total Uptime:(.*)");

    @Nullable
    private String appId;
    private SortedSet<UUID> logGuids = Collections.synchronizedSortedSet(new TreeSet<>());

    @Nullable
    private DateTime lastLogTimestamp = null;

    public ServerlessSparkBatchJob(@NotNull SparkSubmissionParameter submissionParameter,
                                   @NotNull SparkBatchAzureSubmission azureSubmission,
                                   @NotNull Observer<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject) {
        super(submissionParameter, azureSubmission, ctrlSubject);
    }

    @NotNull
    private Observable<? extends AzureSparkServerlessCluster> getCluster() {
        return AzureSparkServerlessClusterManager.getInstance()
                .findCluster(getAzureSubmission().getAccountName(), getAzureSubmission().getClusterId())
                .onErrorReturn(err -> {
                    if (err instanceof NoSuchElementException) {
                        throw propagate(new SparkJobNotConfiguredException(String.format(
                                "Can't find the target cluster %s(ID: %s) from account %s",
                                getSubmissionParameter().getClusterName(),
                                getAzureSubmission().getClusterId(),
                                getAzureSubmission().getAccountName())));
                    }

                    throw propagate(err);
                });
    }

    @NotNull
    @Override
    public Observable<? extends ISparkBatchJob> deploy(@NotNull String artifactPath) {
        return getCluster()
                .flatMap(cluster -> {
                    try {
                        if (cluster.getStorageAccount() == null) {
                            // TODO: May use interaction session to upload
                            return Observable.empty();
                        }

                        File localFile = new File(artifactPath);

                        URI remoteUri = URI.create(cluster.getStorageAccount().getDefaultContainerOrRootPath())
                                .resolve("SparkSubmission/")
                                .resolve(JobUtils.getFormatPathByDate() + "/")
                                .resolve(localFile.getName());

                        ctrlInfo(String.format("Begin uploading file %s to Azure Datalake store %s ...", artifactPath, remoteUri));

                        getSubmissionParameter().setFilePath(remoteUri.toString());

                        return cluster.uploadToStorage(localFile, remoteUri);
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                })
                .doOnNext(size -> ctrlInfo(String.format("Upload to Azure Datalake store %d bytes successfully.", size)))
                .map(path -> this);
    }

    @Nullable
    @Override
    public URI getConnectUri() {
        return getAzureSubmission().getLivyUri() == null ? null : getAzureSubmission().getLivyUri().resolve("/batches");
    }

    // To get a job total up time observable
    private Observable<String> getTotalUpTime(String url) {
        return Observable.fromCallable(() -> {
            String uptime = "";

            try {
                HttpResponse resp = getAzureSubmission().getHttpResponseViaGet(url);

                // FIXME!!! Authentication issue in dogfood
                if (resp.getCode() >= 400) {
                    return String.format("ErrorCode: %d", resp.getCode());
                }

                String html = resp.getContent();

                // The Total Uptime should be looks like:
                //      Total Uptime: 6 s
                // OR
                //      Total Uptime:
                // OR empty
                String totalUptime = Jsoup.parse(html, url).select("li:contains(Total Uptime:)").text();

                Matcher uptimeMatcher = totalUptimeIdPattern.matcher(totalUptime);
                if (uptimeMatcher.find()) {
                    uptime = uptimeMatcher.group(1).trim();
                }
            } catch (IOException ignored) { }

            return uptime;
        })
        .repeatWhen(ob -> ob.delay(10, TimeUnit.SECONDS))
        .takeUntil(StringUtils::isNotBlank)
        .filter(StringUtils::isNotBlank);
    }

    @Override
    public Observable<SimpleImmutableEntry<String, String>> awaitDone() {
        return Observable
                .zip(getCluster().flatMap(AzureSparkServerlessCluster::get),
                     getSparkJobApplicationIdObservable(),
                     (cluster, appIdFound) -> cluster.getSparkHistoryUiUri() == null ?
                             null :
                             cluster.getSparkHistoryUiUri()
                                    .resolve(appIdFound == null ? "/" : String.format("/history/%s/", appIdFound))
                                    .toString() + "?showIncomplete=true&adlaAccountName=" + cluster.getAccount().getName())
                .filter(Objects::nonNull)
                .doOnNext(jobUri -> Observable.just(new SimpleImmutableEntry<>(MessageInfoType.Hyperlink, jobUri))
                                              .subscribeOn(Schedulers.io())
                                              // Delay 10Sec, let the History server be ready
                                              .delay(10, TimeUnit.SECONDS)
                                              .subscribe(msg -> getCtrlSubject().onNext(msg)))
                .flatMap(this::getTotalUpTime)
                .flatMap(uptime -> super.awaitDone());
    }

    // Get logs with paging
    Observable<String> getBatchLogs(int start) {
        return getBatchLogRequest(start, -1)
                .concatMap(jobResp -> {
                    if (jobResp.getFrom() + jobResp.getLog().size() == jobResp.getTotal()) {
                        return Observable.from(jobResp.getLog());
                    }

                    // Pagination
                    return Observable.from(jobResp.getLog())
                            .concatWith(getBatchLogs(jobResp.getFrom() + jobResp.getLog().size()));
                });
    }

    @NotNull
    @Override
    public Observable<SimpleImmutableEntry<MessageInfoType, String>> getSubmissionLog() {
        PublishSubject<String> logTypedWindowSubject = PublishSubject.create();
        AtomicInteger windowIndex = new AtomicInteger();

        return getBatchLogs(0)
                .doOnNext(line -> {
                    logGuids.add(UUID.nameUUIDFromBytes(line.getBytes()));

                    if (line.startsWith("\nstderr:")) {
                        logTypedWindowSubject.onNext("stderr");
                    }
                })
                .window(() -> logTypedWindowSubject)
                .flatMap(obs -> {
                    switch (windowIndex.getAndIncrement()) {
                        case 0: // stdout
                            return obs
                                    .filter(line -> !line.trim().equalsIgnoreCase("stdout:"))
                                    .map(line -> new SimpleImmutableEntry<>(MessageInfoType.Log, line));
                        case 1: // stderr
                            return obs
                                    .filter(line -> !line.startsWith("\nstderr:"))
                                    .map(line -> new SimpleImmutableEntry<>(MessageInfoType.Error, line));
                        default:
                    }

                    return obs.map(line -> new SimpleImmutableEntry<>(MessageInfoType.Info, line));
                });
    }

    @NotNull
    @Override
    public Observable<SimpleImmutableEntry<String, Long>> getDriverLog(@NotNull String type, long logOffset, int size) {
        if (getConnectUri() == null) {
            return Observable.error(new SparkJobNotConfiguredException("Can't get Spark job connection URI, " +
                    "please configure Spark cluster which the Spark job will be submitted."));
        }

        // TODO:: Only allow the sequence reading

        int blockSize = (size >= 0) ? size : 4096;

        return getBatchLogs(0)
                .doOnNext(line -> {
                    Matcher appIdMatcher = appIdPattern.matcher(line);
                    if (appIdMatcher.find()) {
                        setAppId(appIdMatcher.group(1));
                    }
                })
                .skipWhile(line -> {
                    if (type.equals("stdout")) {
                        // Reading from `stdout:`(included)
                        return !line.trim().equalsIgnoreCase("stdout:");
                    } else {
                        // Reading from `\nstderr:`(included)
                        return !line.startsWith("\nstderr:");
                    }
                })
                .skip(1)    // Skip the first line, `stdout:` or `\nstderr:`
                .takeWhile(line -> {
                    if (type.equals("stdout")) {
                        // Stop reading at `\nstderr:`(excluded)
                        return !line.startsWith("\nstderr:");
                    } else {
                        return true;
                    }
                })
                .skipWhile(line -> logGuids.contains(UUID.nameUUIDFromBytes(line.getBytes())))
                // Scan with calculating the remained buffer size
                // Plus one for the linebreak to be added
                .scan(Pair.of(blockSize, (String) null), (remainedSizeWithLine, line) ->
                        Pair.of(remainedSizeWithLine.getLeft() - line.length() - 1, line))
                .filter(remainedSizeWithLine -> remainedSizeWithLine.getRight() != null)
                .takeWhile(remainedSizeWithLine -> remainedSizeWithLine.getLeft() >= 0)
                .map(Pair::getRight)
                .doOnNext(line -> logGuids.add(UUID.nameUUIDFromBytes(line.getBytes())))
                .map(line -> line + "\n")
                .toList()
                .map(logs -> {
                    if (!logs.isEmpty()) {
                        lastLogTimestamp = DateTime.now();
                    }

                    return new SimpleImmutableEntry<>(String.join("", logs), logOffset);
                });
    }

    @Override
    Observable<String> getSparkJobDriverLogUrlObservable() {
        return Observable.just(Objects.requireNonNull(getConnectUri()).toString() + "/" + getBatchId() + "/log");
    }

    @Override
    Observable<String> getSparkJobApplicationIdObservable() {
        return Observable.fromCallable(this::getAppId)
//                .observeOn(Schedulers.io())
                .repeatWhen(ob -> ob.delay(1, TimeUnit.SECONDS))
                .takeUntil(Objects::nonNull)
                .filter(Objects::nonNull);
    }

    @NotNull
    @Override
    public Observable<String> awaitPostDone() {
        final int waitingSeconds = 30;

        return Observable.fromCallable(() -> Observable.interval(1, TimeUnit.SECONDS, Schedulers.computation())
                .map(i -> getLastLogTimestamp())
                .filter(Objects::nonNull)
                .takeUntil(last -> last.isBefore(DateTime.now().minusSeconds(waitingSeconds)))
                .filter(last -> last.isBefore(DateTime.now().minusSeconds(waitingSeconds)))
                .map(any -> String.format("No logs for %d seconds", waitingSeconds))
                .toBlocking()
                .firstOrDefault(null));
    }

    @Nullable
    private DateTime getLastLogTimestamp() {
        return lastLogTimestamp;
    }

    @NotNull
    private SparkBatchAzureSubmission getAzureSubmission() {
        return (SparkBatchAzureSubmission) getSubmission();
    }

    @Nullable
    public String getAppId() {
        return appId;
    }

    public void setAppId(@Nullable String appId) {
        this.appId = appId;
    }

    private void ctrlInfo(@NotNull String message) {
        getCtrlSubject().onNext(new SimpleImmutableEntry<>(MessageInfoType.Info, message));
    }
}
