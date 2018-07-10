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

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeMap;
import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azure.hdinsight.sdk.common.azure.serverless.AzureSparkServerlessCluster;
import com.microsoft.azure.hdinsight.sdk.common.azure.serverless.AzureSparkServerlessClusterManager;
import com.microsoft.azure.hdinsight.spark.jobs.JobUtils;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import rx.Observable;
import rx.Observer;
import rx.schedulers.Schedulers;

import java.io.File;
import java.net.URI;
import java.util.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class ServerlessSparkBatchJob extends SparkBatchJob {
//    private final String STANDALONE_SPARK_APPID_REGEX = "Connected to Spark cluster with app ID (\\S+)";
    private final String STANDALONE_SPARK_APPID_REGEX = "(app-[-\\d]+)";
    private final Pattern appIdPattern = Pattern.compile(STANDALONE_SPARK_APPID_REGEX);
    @Nullable
    private String appId;
    private int driverLogStart = 0;

    private TreeRangeMap<Long, Integer> driverLogRangeLines = TreeRangeMap.create();

    public ServerlessSparkBatchJob(@NotNull SparkSubmissionParameter submissionParameter,
                                   @NotNull SparkBatchAzureSubmission azureSubmission,
                                   @NotNull Observer<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject) {
        super(submissionParameter, azureSubmission, ctrlSubject);
    }

    @NotNull
    private Observable<? extends AzureSparkServerlessCluster> getCluster() {
        return AzureSparkServerlessClusterManager.getInstance()
                .findCluster(getAzureSubmission().getAccountName(), getAzureSubmission().getClusterId());
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

    @NotNull
    @Override
    public Observable<String> awaitStarted() {
        return super.awaitStarted()
                .flatMap(state -> Observable.zip(
                        getCluster(), getSparkJobApplicationIdObservable().defaultIfEmpty(null),
                        (cluster, appId) -> Pair.of(
                                state,
                                cluster.getSparkHistoryUiUri() == null ?
                                        null :
                                        cluster.getSparkHistoryUiUri()
                                               .resolve(appId == null ?
                                                        "/" :
                                                        String.format("/history/%s/", appId))
                                               .toString() + "?adlaAccountName=" + cluster.getAccount().getName())))
                .map(stateJobUriPair -> {
                    if (stateJobUriPair.getRight() != null) {
                        getCtrlSubject().onNext(new SimpleImmutableEntry<>(MessageInfoType.Hyperlink,
                                                                           stateJobUriPair.getRight()));
                    }

                    return stateJobUriPair.getKey();
                });
    }

    @NotNull
    @Override
    public Observable<SimpleImmutableEntry<MessageInfoType, String>> getSubmissionLog() {
        return super.getSubmissionLog()
                .scan(Pair.of(-1, (SimpleImmutableEntry<MessageInfoType, String>) null), (lastIndexedLog, typedLog) ->
                        Pair.of(lastIndexedLog.getLeft() + 1, typedLog))
                .filter(indexedTypeLog -> indexedTypeLog.getRight() != null)
                .takeUntil(indexedTypeLog -> {
                    Matcher m = appIdPattern.matcher(indexedTypeLog.getRight().getValue());

                    if (m.find()) {
                        appId = m.group(1);
                        driverLogStart = indexedTypeLog.getLeft() + 1;

                        return true;
                    }

                    return false;
                })
                .map(Pair::getRight);
    }

    @NotNull
    @Override
    public Observable<SimpleImmutableEntry<String, Long>> getDriverLog(@NotNull String type, long logOffset, int size) {
        if (getConnectUri() == null) {
            return Observable.error(new SparkJobNotConfiguredException("Can't get Spark job connection URI, " +
                    "please configure Spark cluster which the Spark job will be submitted."));
        }

        Map.Entry<Range<Long>, Integer> prevLineEntry = driverLogRangeLines.getEntry(logOffset - 1);

        // FIXME!!! logOffset is not the line header isn't handled

        int nextLine;
        Range<Long> lastRange;

        if (prevLineEntry != null) {
            lastRange = prevLineEntry.getKey();
            nextLine = prevLineEntry.getValue();
        } else {
            Map.Entry<Range<Long>, Integer> startEntry = getLastLogRangeIndex();

            if (startEntry != null) {
                // Start from the last log index
                nextLine = startEntry.getValue() + 1;
                lastRange = startEntry.getKey();
            } else {
                // Start from the scratch
                nextLine = driverLogStart;
                lastRange = Range.closed(-1L, -1L);
            }
        }

        return Observable.just(Pair.of(lastRange, nextLine))
                .flatMap(prevRangeLinePair -> getBatchLogs(prevRangeLinePair.getValue() + 1, max(min(size / 128 + 1, 16), 16))
                        .flatMap(Observable::from)
                        .scan(Triple.of(prevRangeLinePair.getLeft(), prevRangeLinePair.getRight(), (String)null),
                                (lastRangeLineLogTriple , log) -> {
                                    Long currentStart = lastRangeLineLogTriple.getLeft().upperEndpoint() + 1;
                                    int currentLine = lastRangeLineLogTriple.getMiddle() + 1;

                                    // With line-break added
                                    Range<Long> logRange = Range.closed(currentStart, currentStart + log.length());
                                    driverLogRangeLines.put(logRange, currentLine);

                                    return Triple.of(logRange, currentLine, log + "\n");
                                })
                        .filter(rangeLineLogTriple -> rangeLineLogTriple.getRight() != null)
                )
                .skipWhile(indexedLog -> !indexedLog.getLeft().contains(logOffset))
                .map(Triple::getRight)
                .toList()
                .map(logs -> new SimpleImmutableEntry<>(String.join("", logs), logOffset));
    }

    @Nullable
    private Map.Entry<Range<Long>, Integer> getLastLogRangeIndex() {
        return driverLogRangeLines.asDescendingMapOfRanges().entrySet().stream().findFirst().orElse(null);
    }

    @Override
    Observable<String> getSparkJobDriverLogUrlObservable() {
        return Observable.just(Objects.requireNonNull(getConnectUri()).toString() + "/" + getBatchId() + "/log");
    }

    @Override
    Observable<String> getSparkJobApplicationIdObservable() {
        return appId == null ? Observable.empty() : Observable.just(appId);
    }

    @NotNull
    @Override
    public Observable<String> awaitPostDone() {
        return Observable.interval(3, TimeUnit.SECONDS)//, Schedulers.computation())
                .map(i -> getLastLogRangeIndex())
                .scan(Pair.of(null, null), (prev2Entries, current) -> Pair.of(prev2Entries.getRight(), current))
                .takeUntil(prevAndCurrentEntries -> prevAndCurrentEntries.getLeft() != prevAndCurrentEntries.getRight())
                .filter(prevAndCurrentEntries -> prevAndCurrentEntries.getLeft() != prevAndCurrentEntries.getRight())
                .map(any -> "pseudeo done");
    }

    @NotNull
    private SparkBatchAzureSubmission getAzureSubmission() {
        return (SparkBatchAzureSubmission) getSubmission();
    }

    private void ctrlInfo(@NotNull String message) {
        getCtrlSubject().onNext(new SimpleImmutableEntry<>(MessageInfoType.Info, message));
    }
}
