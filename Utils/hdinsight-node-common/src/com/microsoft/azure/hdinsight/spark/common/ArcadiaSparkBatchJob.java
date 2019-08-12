package com.microsoft.azure.hdinsight.spark.common;

import rx.Observable;
import rx.Observer;

import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;

import java.io.File;
import java.net.URI;
import java.util.AbstractMap.SimpleImmutableEntry;

public class ArcadiaSparkBatchJob extends SparkBatchJob {
    private final @NotNull Deployable deployDelegate;

    public ArcadiaSparkBatchJob(final @NotNull SparkSubmissionParameter submissionParameter,
                                final @NotNull SparkBatchSubmission sparkBatchSubmission,
                                final @NotNull Deployable deployDelegate,
                                final @NotNull Observer<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject) {
        super(submissionParameter, sparkBatchSubmission, ctrlSubject);
        this.deployDelegate = deployDelegate;
    }

    @NotNull
    @Override
    public Observable<? extends ISparkBatchJob> deploy(@NotNull String artifactPath) {
        return deployDelegate.deploy(new File(artifactPath))
                .map(uploadedUri -> {
                    ctrlInfo(String.format("File %s has been uploaded to %s.", artifactPath, uploadedUri));

                    getSubmissionParameter().setFilePath(uploadedUri);

                    return this;
                });
    }

    @NotNull
    @Override
    public Observable<String> awaitStarted() {
        // No submission log and driver log fetching supports
        return Observable.just("no_waiting")
                .doOnNext(state -> ctrlInfo("The Spark Batch job has been submitted to Arcadia "
                        + getConnectUri().toString()
                        + " with the following parameters: "
                        + getSubmissionParameter().serializeToJson()));
    }

    @NotNull
    @Override
    public Observable<SimpleImmutableEntry<String, Long>> getDriverLog(String type, long logOffset, int size) {
        return Observable.empty();
    }

    @NotNull
    @Override
    public Observable<SimpleImmutableEntry<MessageInfoType, String>> getSubmissionLog() {
        // No batches/{id}/log API support yet
        return Observable.empty();
    }

    @NotNull
    @Override
    protected Observable<SimpleImmutableEntry<String, String>> getJobDoneObservable() {
        // TODO: Monitoring the Job status
        return Observable.empty();
    }

    @Nullable
    @Override
    public URI getConnectUri() {
        return getArcadiaSubmission().getLivyUri().resolve("batches");
    }

    @NotNull
    private SparkBatchArcadiaSubmission getArcadiaSubmission() {
        return (SparkBatchArcadiaSubmission) super.getSubmission();
    }

    private void ctrlInfo(@NotNull String message) {
        getCtrlSubject().onNext(new SimpleImmutableEntry<>(MessageInfoType.Info, message));
    }
}
