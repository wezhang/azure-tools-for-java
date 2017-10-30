/*
 * Copyright (c) Microsoft Corporation
 * <p/>
 * All rights reserved.
 * <p/>
 * MIT License
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.hdinsight.spark.run;

import com.google.common.net.HostAndPort;
import com.intellij.execution.ExecutionException;
import com.intellij.openapi.compiler.CompilationException;
import com.intellij.openapi.compiler.CompileScope;
import com.intellij.openapi.compiler.CompilerManager;
import com.intellij.openapi.compiler.CompilerMessageCategory;
import com.intellij.openapi.project.Project;
import com.intellij.packaging.artifacts.Artifact;
import com.intellij.packaging.impl.compiler.ArtifactCompileScope;
import com.intellij.packaging.impl.compiler.ArtifactsWorkspaceSettings;
import com.intellij.remote.RemoteProcess;
import com.microsoft.azure.hdinsight.common.ClusterManagerEx;
import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.sdk.common.HDIException;
import com.microsoft.azure.hdinsight.sdk.common.NotSupportExecption;
import com.microsoft.azure.hdinsight.spark.common.*;
import com.microsoft.azure.hdinsight.spark.jobs.JobUtils;
import hidden.edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.commons.io.output.NullOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import rx.Observable;
import rx.Single;
import rx.SingleSubscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.microsoft.azure.hdinsight.common.MessageInfoType.Info;
import static rx.exceptions.Exceptions.propagate;

public class SparkBatchJobRemoteProcess extends RemoteProcess {
    @NotNull
    private Project project;
    @NotNull
    private SparkSubmitModel submitModel;
    @NotNull
    private final PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject;
    @NotNull
    private SparkJobLogInputStream jobStdoutLogInputSteam;
    @NotNull
    private SparkJobLogInputStream jobStderrLogInputSteam;

    private boolean isDisconnected;
    private Subscription jobMonitorSubscription;

    private final Object lock = new Object();
    @Nullable
    private Subscription ctrlLogSubscription;

    public SparkBatchJobRemoteProcess(@NotNull Project project, @NotNull SparkSubmitModel sparkSubmitModel,
                                      @NotNull PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject)
            throws ExecutionException {
        this.project = project;
        this.submitModel = sparkSubmitModel;
        this.ctrlSubject = ctrlSubject;

        this.jobStdoutLogInputSteam = new SparkJobLogInputStream("stdout");
        this.jobStderrLogInputSteam = new SparkJobLogInputStream("stderr");
    }

    /**
     * To Kill the remote job.
     *
     * @return is the remote Spark Job killed
     */
    @Override
    public boolean killProcessTree() {
        return false;
    }

    /**
     * Is the Spark job session connected
     *
     * @return is the Spark Job log getting session still connected
     */
    @Override
    public boolean isDisconnected() {
        return isDisconnected;
    }

    @Nullable
    @Override
    public HostAndPort getLocalTunnel(int i) {
        return null;
    }

    @Override
    public OutputStream getOutputStream() {
        return new NullOutputStream();
    }

    @Override
    public InputStream getInputStream() {
        return jobStdoutLogInputSteam;
    }

    @Override
    public InputStream getErrorStream() {
        return jobStderrLogInputSteam;
    }

    @Override
    public int waitFor() throws InterruptedException {
        return 0;
    }

    @Override
    public int exitValue() {
        return 0;
    }

    @Override
    public void destroy() {

        this.stop();
    }

    // TODO: Move the function to HDInsightHelper
    Single<Artifact> buildArtifact() {
        return Single.create(ob -> {
            if (submitModel.isLocalArtifact()) {
                ob.onError(new NotSupportExecption());
                return;
            }

            final Set<Artifact> artifacts = Collections.singleton(submitModel.getArtifact());
            ArtifactsWorkspaceSettings.getInstance(project).setArtifactsToBuild(artifacts);

            final CompileScope scope = ArtifactCompileScope.createArtifactsScope(project, artifacts, true);
            // Make is an async work
            CompilerManager.getInstance(project).make(scope, (aborted, errors, warnings, compileContext) -> {
                if (aborted || errors != 0) {
                    ob.onError(new CompilationException(Arrays.toString(compileContext.getMessages(CompilerMessageCategory.ERROR))));
                } else {
                    ob.onSuccess(submitModel.getArtifact());
                }
            });
        });
    }

    // TODO: Move the function to HDICommon
    Single<IClusterDetail> deployArtifact(@NotNull Artifact artifact, @NotNull String clusterName) {
        return Single.create(ob -> {
            try {
                IClusterDetail clusterDetail = ClusterManagerEx.getInstance()
                        .getClusterDetailByName(clusterName)
                        .orElseThrow(() -> new HDIException("No cluster name matched selection: " + clusterName));

                ctrlSubject.onNext(new SimpleImmutableEntry<>(Info, "Deploy the jar file into cluster..."));

                String jobArtifactUri = JobUtils.uploadFileToCluster(
                        clusterDetail,
                        submitModel.getArtifactPath(artifact.getName())
                                .orElseThrow(() -> new SparkJobException("Can't find jar path to upload")),
                        ctrlSubject);

                submitModel.getSubmissionParameter().setFilePath(jobArtifactUri);

                ob.onSuccess(clusterDetail);
            } catch (Exception e) {
                ob.onError(e);
            }
        });
    }

    Single<SparkBatchJob> submit(@NotNull IClusterDetail cluster, @NotNull SparkSubmissionParameter parameter) {
        return Single.create((SingleSubscriber<? super SparkBatchJob> ob) -> {
            try {
                SparkBatchSubmission.getInstance().setCredentialsProvider(cluster.getHttpUserName(), cluster.getHttpPassword());

                SparkBatchJob sparkJob = new SparkBatchJob(
                        URI.create(SparkSubmitHelper.getLivyConnectionURL(cluster)),
                        parameter,
                        SparkBatchSubmission.getInstance());

                // would block a while
                ctrlSubject.onNext(new SimpleImmutableEntry<>(Info, "The Spark job is submitting ..."));

                sparkJob.createBatchJob();
                ob.onSuccess(sparkJob);
            } catch (Exception e) {
                ob.onError(e);
            }
        });
    }

    public void start() {
        // Monitoring the job status
        buildArtifact()
                .flatMap(artifact -> deployArtifact(artifact, submitModel.getSubmissionParameter().getClusterName()).subscribeOn(Schedulers.io()))
                .flatMap(cluster -> submit(cluster, submitModel.getSubmissionParameter()).subscribeOn(Schedulers.io()))
                .map(job -> {
                    try {
                        ctrlLogSubscription = job.getSubmissionLog()
                                .subscribeOn(Schedulers.io())
                                .subscribe(ctrlSubject::onNext, ctrlSubject::onError);

                        jobStderrLogInputSteam.attachJob(job);
                        jobStdoutLogInputSteam.attachJob(job);
                    } catch (IOException e) {
                        throw propagate(e);
                    }

                    return job;
                })
                .subscribe(this::startJobMonitor, this.ctrlSubject::onError);
    }

    public void stop() {
        this.isDisconnected = true;
        Optional.ofNullable(this.ctrlLogSubscription).ifPresent(Subscription::unsubscribe);

        this.ctrlSubject.onCompleted();
    }

    private void startJobMonitor(SparkBatchJob job) {
        this.jobMonitorSubscription = Observable.interval(200, TimeUnit.MILLISECONDS)
                .map((times) -> {
                    try {
                        return job.getState();
                    } catch (IOException e) {
                        throw propagate(e);
                    }
                })
                .map(s -> SparkBatchJobState.valueOf(s.toUpperCase()))
                .map(state -> {
                            switch(state) {
                                case NOT_STARTED:
                                case STARTING:
                                case RUNNING:
                                case RECOVERING:
                                case BUSY:
                                case IDLE:
                                    break;
                                case SHUTTING_DOWN:
                                case ERROR:
                                case DEAD:
                                case SUCCESS:
                                default:
                                    return true;
                            }
                            return false;
                        })
                .filter((isJobStop) -> {
                    try {
                        return isJobStop && job.isLogAggregated();
                    } catch (IOException e) {
                        throw propagate(e);
                    }
                })
                .delay(3, TimeUnit.SECONDS)
                .filter((isJobStop) -> {
                    try {
                        return jobStderrLogInputSteam.available() == 0 && jobStdoutLogInputSteam.available() == 0;
                    } catch (IOException e) {
                        throw propagate(e);
                    }
                })
                .delay(3, TimeUnit.SECONDS)
                .subscribe(
                        jobStop -> stopJobMonitor(),
                        err -> stopJobMonitor());
    }

    private void stopJobMonitor() {
        Optional.ofNullable(this.jobMonitorSubscription)
                .ifPresent(sub -> {
                    sub.unsubscribe();
                    stop();
                });
    }
}
