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

import com.intellij.execution.ExecutionException;
import com.intellij.openapi.project.Project;
import com.jcraft.jsch.JSchException;
import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.spark.common.*;
import com.microsoft.azure.hdinsight.spark.jobs.JobUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;

import static rx.exceptions.Exceptions.propagate;

public class SparkBatchJobRemoteDebugProcess extends SparkBatchJobRemoteProcess {
//    @NotNull
//    private SparkJobDebugLogInputStream jobDebugStdoutLogInputSteam = new SparkJobDebugLogInputStream(super.getInputStream());
//    @NotNull
//    private SparkJobDebugLogInputStream jobDebugStderrLogInputSteam = new SparkJobDebugLogInputStream(super.getErrorStream());

    // Control subjects
    @NotNull
    private final PublishSubject<SparkBatchJobSubmissionEvent> debugEventSubject = PublishSubject.create();

    public SparkBatchJobRemoteDebugProcess(@NotNull Project project,
                                           @NotNull SparkSubmitModel sparkSubmitModel,
                                           @NotNull PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject)
            throws ExecutionException {
        super(project, sparkSubmitModel, ctrlSubject);

        if (!sparkSubmitModel.getAdvancedConfigModel().enableRemoteDebug) {
            throw new ExecutionException(
                    new SparkSubmitAdvancedConfigModel.NotAdvancedConfig("SSH authentication not set"));
        }


//        this.jobDebugStderrLogInputSteam.getExecutorSubject()
//                .subscribe(executor -> getSparkJob()
//                        .ifPresent(job -> debugEventSubject.onNext(
//                                new SparkBatchJobExecutorCreatedEvent(job, executor.host, executor.containerId)
//                        )));
    }

//    @Override
//    public InputStream getInputStream() {
//        return jobDebugStdoutLogInputSteam;
//    }
//
//    @Override
//    public InputStream getErrorStream() {
//        return jobDebugStderrLogInputSteam;
//    }

    @NotNull
    @Override
    public PublishSubject<SparkBatchJobSubmissionEvent> getEventSubject() {
        return debugEventSubject;
    }

    @Override
    public SparkBatchJob createJobToSubmit(IClusterDetail cluster) {
        try {
            return SparkBatchRemoteDebugJob.factory(
                    URI.create(JobUtils.getLivyConnectionURL(cluster)).toString(),
                    getSubmitModel().getSubmissionParameter(),
                    SparkBatchSubmission.getInstance());
        } catch (DebugParameterDefinedException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    protected Observable<SimpleImmutableEntry<SparkBatchJobState, String>> awaitForJobDone(SparkBatchJob runningJob) {
        return createDebugSession((SparkBatchRemoteDebugJob) runningJob)
                .subscribeOn(getSchedulers().processBarVisibleAsync("Create Spark batch job debug session"))
                .flatMap(super::awaitForJobDone);
    }

    private Observable<SparkBatchRemoteDebugJob> createDebugSession(SparkBatchRemoteDebugJob job) {
        return Observable.fromCallable(() -> {
            try {
                SparkBatchDebugSession session = createSparkBatchDebugSession(
                        job.getConnectUri().toString(),
                        getSubmitModel().getAdvancedConfigModel())
                        .open();
                String remoteHost = job.getSparkDriverHost();
                int remotePort = job.getSparkDriverDebuggingPort();

                int localPort = session
                        .forwardToRemotePort(remoteHost, remotePort)
                        .getForwardedLocalPort(remoteHost, remotePort);

                SparkBatchDebugJobJdbPortForwardedEvent jdbReadyEvent = new SparkBatchDebugJobJdbPortForwardedEvent(
                        job, session, remoteHost, remotePort, localPort);

                // Debug session created and SSH port forwarded
                debugEventSubject.onNext(jdbReadyEvent);

                return job;
            } catch (JSchException e) {
                throw new ExecutionException(
                        "Can't create Spark Job remote debug session, " +
                                "please check your SSH connect with manually login.",
                        e);
            } catch (SparkJobException | IOException e) {
                throw new ExecutionException(e);
            }
        });
    }

    /*
     * Create a Spark Batch Job Debug Session with SSH certification
     */
    static public SparkBatchDebugSession createSparkBatchDebugSession(String connectionUrl,
                                                                      @NotNull SparkSubmitAdvancedConfigModel advModel)
            throws SparkJobException, JSchException {
        String sshServer = getSshHost(connectionUrl);

        SparkBatchDebugSession session = SparkBatchDebugSession.factory(sshServer, advModel.sshUserName);

        switch (advModel.sshAuthType) {
            case UseKeyFile:
                session.setPrivateKeyFile(advModel.sshKeyFile);
                break;
            case UsePassword:
                session.setPassword(advModel.sshPassword);
                break;
            default:
                throw new SparkSubmitAdvancedConfigModel.UnknownSSHAuthTypeException(
                        "Unknown SSH authentication type: " + advModel.sshAuthType.name());
        }

        return session;
    }

    /**
     * Get SSH Host from the HDInsight connection URL
     *
     * @param connectionUrl the HDInsight connection URL, such as: https://spkdbg.azurehdinsight.net/batch
     * @return SSH host
     * @throws URISyntaxException connection URL is invalid
     */
    private static String getSshHost(String connectionUrl) {
        URI connectUri = URI.create(connectionUrl);
        String segs[] = connectUri.getHost().split("\\.");
        segs[0] = segs[0].concat("-ssh");
        return StringUtils.join(segs, ".");
    }

    /**
     * To get Executor from Yarn UI App Attempt page
     */
    private Observable<SimpleEntry<String, String>> getExecutorsObservable(@NotNull SparkBatchRemoteDebugJob sparkDebugJob) {
        return sparkDebugJob
                .getSparkJobYarnCurrentAppAttempt()
                .flatMap(appAttempt -> sparkDebugJob.getSparkJobYarnContainersObservable(appAttempt)
                        .filter(hostContainerPair -> !StringUtils.equals(
                                hostContainerPair.getValue(), appAttempt.getContainerId())))
                .map(kv -> new SimpleEntry<>(kv.getKey(), kv.getValue()));
    }

}
