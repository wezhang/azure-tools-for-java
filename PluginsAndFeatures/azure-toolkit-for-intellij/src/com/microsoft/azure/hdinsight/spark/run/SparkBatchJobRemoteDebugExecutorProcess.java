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

package com.microsoft.azure.hdinsight.spark.run;

import com.intellij.execution.ExecutionException;
import com.intellij.openapi.project.Project;
import com.jcraft.jsch.JSchException;
import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.spark.common.*;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;

public class SparkBatchJobRemoteDebugExecutorProcess extends SparkBatchJobRemoteProcess {
    @NotNull
    private SparkBatchRemoteDebugJob parentJob;
    @NotNull
    private final String host;
    @NotNull
    private final SparkBatchDebugSession debugSshSession;
    @NotNull
    private String logUrl;
    @NotNull
    private SparkJobExecutorLogInputStream stdOutInputStream;
    @NotNull
    private SparkJobExecutorLogInputStream stdErrInputStream;

    public SparkBatchJobRemoteDebugExecutorProcess(@NotNull Project project,
                                                   @NotNull SparkSubmitModel sparkSubmitModel,
                                                   @NotNull SparkBatchRemoteDebugJob parentJob,
                                                   @NotNull String host,
                                                   @NotNull SparkBatchDebugSession debugSshSession,
                                                   @NotNull String logBaseUrl) {

        super(project, sparkSubmitModel, PublishSubject.create());
        this.parentJob = parentJob;
        this.host = host;
        this.debugSshSession = debugSshSession;
        this.logUrl = logBaseUrl;
        this.stdOutInputStream = new SparkJobExecutorLogInputStream("stdout", logBaseUrl);
        this.stdErrInputStream = new SparkJobExecutorLogInputStream("stderr", logBaseUrl);
    }

    @Override
    protected Observable<SimpleImmutableEntry<IClusterDetail, String>> prepareArtifact() {
        return Observable.just(new SimpleImmutableEntry<>(null, "executor, no standalone path"));
    }

    @Override
    protected Observable<? extends SparkBatchJob> submitJob(SimpleImmutableEntry<IClusterDetail, String> clusterArtifactUriPair) {
        return Observable.just(parentJob);
    }

    @Override
    protected Observable<SimpleImmutableEntry<SparkBatchJobState, String>> awaitForJobDone(SparkBatchJob runningJob) {
        return createDebugSession((SparkBatchRemoteDebugJob) runningJob)
                .subscribeOn(getSchedulers().processBarVisibleAsync("Create Spark batch job debug session"))
                .flatMap(super::awaitForJobDone);
    }

    @Override
    public InputStream getInputStream() {
        return stdOutInputStream;
    }

    @Override
    public InputStream getErrorStream() {
        return stdErrInputStream;
    }

    private Observable<SparkBatchRemoteDebugJob> createDebugSession(SparkBatchRemoteDebugJob job) {
        return Observable.fromCallable(() -> {
            try {
                String remoteHost = getHost();
                int remotePort = job.getYarnContainerJDBListenPort(getLogUrl());

                int localPort = getDebugSshSession()
                        .forwardToRemotePort(remoteHost, remotePort)
                        .getForwardedLocalPort(remoteHost, remotePort);

                SparkBatchDebugJobJdbPortForwardedEvent jdbReadyEvent = new SparkBatchDebugJobJdbPortForwardedEvent(
                        job, getDebugSshSession(), remoteHost, remotePort, localPort, false);

                // Debug session created and SSH port forwarded
                getEventSubject().onNext(jdbReadyEvent);

                return job;
            } catch (JSchException e) {
                throw new ExecutionException(
                        "Can't create Spark Job remote debug session, " +
                                "please check your SSH connect with manually login.",
                        e);
            } catch (IOException e) {
                throw new ExecutionException(e);
            }
        });
    }

    @NotNull
    public String getHost() {
        return host;
    }

    @NotNull
    public String getLogUrl() {
        return logUrl;
    }

    @NotNull
    public SparkBatchDebugSession getDebugSshSession() {
        return debugSshSession;
    }
}
