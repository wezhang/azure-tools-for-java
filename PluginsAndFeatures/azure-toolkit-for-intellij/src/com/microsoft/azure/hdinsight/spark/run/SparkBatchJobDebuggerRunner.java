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
 *
 */

package com.microsoft.azure.hdinsight.spark.run;

import com.intellij.debugger.impl.GenericDebuggerRunner;
import com.intellij.debugger.impl.GenericDebuggerRunnerSettings;
import com.intellij.execution.ExecutionException;
import com.intellij.execution.Executor;
import com.intellij.execution.configurations.*;
import com.intellij.execution.executors.DefaultDebugExecutor;
import com.intellij.execution.process.*;
import com.intellij.execution.runners.ExecutionEnvironment;
import com.intellij.execution.runners.ExecutionEnvironmentBuilder;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Key;
import com.jcraft.jsch.JSchException;
import com.microsoft.azure.hdinsight.common.ClusterManagerEx;
import com.microsoft.azure.hdinsight.common.HDInsightUtil;
import com.microsoft.azure.hdinsight.common.JobStatusManager;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.spark.common.*;
import com.microsoft.azure.hdinsight.spark.jobs.JobUtils;
import com.microsoft.azure.hdinsight.spark.run.configuration.RemoteDebugRunConfiguration;
import com.microsoft.azuretools.telemetry.AppInsightsClient;
import com.microsoft.intellij.hdinsight.messages.HDInsightBundle;
import org.apache.commons.lang.StringUtils;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import rx.Observable;
import rx.Single;
import rx.Subscriber;
import rx.Subscription;
import rx.exceptions.CompositeException;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.Phaser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static rx.exceptions.Exceptions.propagate;

public class SparkBatchJobDebuggerRunner extends GenericDebuggerRunner {
    private SparkBatchRemoteDebugJob debugJob;

    public void setDebugJob(SparkBatchRemoteDebugJob debugJob) {
        this.debugJob = debugJob;
    }

    public SparkBatchRemoteDebugJob getDebugJob() {
        return debugJob;
    }

    private static final Key<String> DebugTargetKey = new Key<>("debug-target");
    private static final Key<String> ProfileNameKey = new Key<>("profile-name");
    private static final String DebugDriver = "driver";
    private static final String DebugExecutor = "executor";

    private SparkBatchDebugSession debugSession;
    private boolean isAppInsightEnabled = true;
    private Phaser debugProcessPhaser;

    // More complex pattern, please use grok
    private final Pattern simpleLogPattern = Pattern.compile("\\d{1,2}[/-]\\d{1,2}[/-]\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2} (INFO|WARN|ERROR) .*", Pattern.DOTALL);
    private final Pattern executorLogUrlPattern = Pattern.compile("^\\s+SPARK_LOG_URL_STDERR -> https?://([^:]+):?\\d*/node/containerlogs/(container.*)/livy/stderr.*");


    @Override
    public boolean canRun(@NotNull String executorId, @NotNull RunProfile profile) {
        // Only support debug now, will enable run in future
        return DefaultDebugExecutor.EXECUTOR_ID.equals(executorId) && profile instanceof RemoteDebugRunConfiguration;
    }

    @NotNull
    @Override
    public String getRunnerId() {
        return "SparkBatchJobDebug";
    }

    @Override
    public GenericDebuggerRunnerSettings createConfigurationData(ConfigurationInfoProvider settingsProvider) {
        return null;
    }

    private ExecutionEnvironment buildChildEnvironment(@NotNull ExecutionEnvironment environment,
                                                       String host,
                                                       boolean isDriver) {
        String savedProfileName = environment.getUserData(ProfileNameKey);
        String originProfileName = savedProfileName == null ? environment.getRunProfile().getName() : savedProfileName;

        RunConfiguration driverRunConfiguration = ((RunConfiguration) environment.getRunProfile()).clone();
        driverRunConfiguration.setName(originProfileName + " [" + (isDriver ? "Driver " : "Executor ") + host + "]");

        ExecutionEnvironment childEnv = new ExecutionEnvironmentBuilder(environment).runProfile(driverRunConfiguration)
                .build();

        childEnv.putUserData(DebugTargetKey, isDriver ? DebugDriver : DebugExecutor);
        childEnv.putUserData(ProfileNameKey, originProfileName);

        return childEnv;
    }

    private Single<SimpleEntry<SparkBatchRemoteDebugJob, IClusterDetail>>
    buildDebugSession(@NotNull SparkSubmitModel submitModel) {
        SparkSubmissionParameter submissionParameter = submitModel.getSubmissionParameter();

        return submitModel
                .buildArtifactObservable(submissionParameter.getArtifactName())
                .flatMap(artifact -> {
                    IClusterDetail clusterDetail = Optional.ofNullable(submitModel.getSelectedClusterDetail())
                            .orElseGet(() -> ClusterManagerEx.getInstance()
                                    .getClusterDetailsWithoutAsync(true, submitModel.getProject())
                                    .stream()
                                    .filter(cluster -> cluster.getName().equals(submissionParameter.getClusterName()))
                                    .findFirst()
                                    .orElse(null));

                    return Single.just(new SimpleEntry<>(artifact, clusterDetail));
                })
                .flatMap(pair -> submitModel.deployArtifactObservable(pair.getKey(), pair.getValue())
                        .subscribeOn(Schedulers.io()))
                .map((selectedClusterDetail) -> {
                    // Create Batch Spark Debug Job
                    try {
                        return new SimpleEntry<>(
                                submitModel.tryToCreateBatchSparkDebugJob(selectedClusterDetail),
                                selectedClusterDetail);
                    } catch (Exception e) {
                        HDInsightUtil.setJobRunningStatus(submitModel.getProject(), false);
                        throw propagate(e);
                    }
                })
                .map(pair -> {
                    SparkBatchRemoteDebugJob remoteDebugJob = pair.getKey();
                    IClusterDetail clusterDetail = pair.getValue();

                    try {
                        SparkBatchDebugSession session = createDebugSession(clusterDetail.getConnectionUrl(),
                                submitModel.getAdvancedConfigModel()).open();

                        setDebugSession(session);
                        setDebugJob(remoteDebugJob);
                        return new SimpleEntry<>(remoteDebugJob, clusterDetail);
                    } catch (Exception ex) {
                        throw propagate(ex);
                    }
                });
    }

    public SparkBatchDebugSession getDebugSession() {
        return debugSession;
    }

    public void setDebugSession(SparkBatchDebugSession debugSession) {
        this.debugSession = debugSession;
    }


    private void postAppInsightDebugSuccessEvent() {
        if (!isAppInsightEnabled) {
            return;
        }

        Map<String, String> postEventProperty = new HashMap<>();

        postEventProperty.put("IsSubmitSucceed", "true");
        AppInsightsClient.create(
                HDInsightBundle.message("SparkRunConfigDebugButtonClick"), null,
                postEventProperty);
    }

    private void postAppInsightDebugErrorEvent(String errorMessage) {
        if (!isAppInsightEnabled) {
            return;
        }

        Map<String, String> postEventProperty = new HashMap<>();

        postEventProperty.put("IsSubmitSucceed", "false");
        postEventProperty.put("SubmitFailedReason", HDInsightUtil.normalizeTelemetryMessage(errorMessage));
        AppInsightsClient.create(
                HDInsightBundle.message("SparkRunConfigDebugButtonClick"),
                null,
                postEventProperty);
    }

    @Override
    protected void execute(@NotNull ExecutionEnvironment environment, @Nullable Callback callback, @NotNull RunProfileState state) throws ExecutionException {
        SparkBatchJobSubmissionState submissionState = (SparkBatchJobSubmissionState) state;
        SparkSubmitModel submitModel = submissionState.getSubmitModel();
        Project project = submitModel.getProject();
        JobStatusManager jobStatusMgmt = HDInsightUtil.getSparkSubmissionToolWindowManager(project)
                .getJobStatusManager();

        // Reset the debug process Phaser
        debugProcessPhaser = new Phaser(1);

        Observable.create((Observable.OnSubscribe<String>) ob ->
            buildDebugSession(submitModel).subscribe(debugJobClusterPair-> {
                final SparkBatchRemoteDebugJob remoteDebugJob = debugJobClusterPair.getKey();
                final IClusterDetail clusterDetail = debugJobClusterPair.getValue();
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                String driverHost = null;
                int driverDebugPort = 0;
                String logUrl = null;

                try {
                    jobStatusMgmt.resetJobStateManager();
                    jobStatusMgmt.setJobRunningState(true);

                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
                            clusterDetail.getHttpUserName(), clusterDetail.getHttpPassword()));

                    driverHost = remoteDebugJob.getSparkDriverHost();
                    driverDebugPort = remoteDebugJob.getSparkDriverDebuggingPort();
                    logUrl = remoteDebugJob.getSparkJobDriverLogUrl(
                            remoteDebugJob.getConnectUri(), remoteDebugJob.getBatchId());
                } catch (Exception ex) {
                    ob.onError(ex);
                }

                // Create Driver debug process
                createDebugProcess(
                        environment,
                        callback,
                        submissionState,
                        true,
                        ob,
                        debugProcessPhaser,
                        driverHost,
                        driverDebugPort,
                        logUrl,
                        credentialsProvider
                );

                ob.onNext("Info: Spark Job Driver debugging started.");

                Subscription livyLogSubscription = submitModel
                        .jobLogObservable(remoteDebugJob.getBatchId(), clusterDetail)
                        .subscribeOn(Schedulers.io())
                        .subscribe();

                debugProcessPhaser.arriveAndAwaitAdvance();
                ob.onCompleted();

                livyLogSubscription.unsubscribe();
            }))
        .subscribe(
                info -> HDInsightUtil.showInfoOnSubmissionMessageWindow(project, info),
                throwable -> {
                    String errorMessage;

                    // The throwable may be composed by several exceptions
                    if (throwable instanceof CompositeException) {
                        CompositeException exceptions = (CompositeException) throwable;

                        errorMessage = exceptions.getExceptions().stream()
                                .map(Throwable::getMessage)
                                .collect(Collectors.joining("; "));
                    } else {
                        errorMessage = throwable.getMessage();
                    }

                    HDInsightUtil.showErrorMessageOnSubmissionMessageWindow(
                            project, "Error : Spark batch Job remote debug failed, got exception: " + errorMessage);

                    postAppInsightDebugErrorEvent(errorMessage);
                    debugProcessPhaser.forceTermination();
                    HDInsightUtil.setJobRunningStatus(project, false);
                },
                () -> {
                    jobStatusMgmt.setJobKilled();
                    stopDebugJob();

                    // Spark Job is done
                    HDInsightUtil.showInfoOnSubmissionMessageWindow(
                            submitModel.getProject(), "Info : Debugging Spark batch job in cluster is done.");
                    HDInsightUtil.setJobRunningStatus(project, false);
                }
        );
    }

    private void stopDebugJob() {
        if (getDebugSession() != null) {
            getDebugSession().close();

            postAppInsightDebugSuccessEvent();
        }

        if (getDebugJob() != null) {
            try {
                getDebugJob().killBatchJob();
            } catch (IOException ignore) { }
        }
    }

    /**
     * Get SSH Host from the HDInsight connection URL
     *
     * @param connectionUrl the HDInsight connection URL, such as: https://spkdbg.azurehdinsight.net/batch
     * @return SSH host
     * @throws URISyntaxException connection URL is invalid
     */
    protected String getSshHost(String connectionUrl) throws URISyntaxException {
        URI connectUri = new URI(connectionUrl);
        String segs[] = connectUri.getHost().split("\\.");
        segs[0] = segs[0].concat("-ssh");
        return StringUtils.join(segs, ".");
    }

    SparkBatchDebugSession createDebugSession(
            String connectionUrl,
            SparkSubmitAdvancedConfigModel advModel) throws SparkJobException, JSchException {
        if (advModel == null) {
            throw new SparkSubmitAdvancedConfigModel.NotAdvancedConfig("SSH authentication not set");
        }

        String sshServer;

        try {
            sshServer = getSshHost(connectionUrl);
        } catch (URISyntaxException e) {
            throw new SparkJobException("Connection URL is not valid: " + connectionUrl);
        }

        SparkBatchDebugSession session = SparkBatchDebugSession.factory(sshServer, advModel.sshUserName);

        switch (advModel.sshAuthType) {
            case UseKeyFile:
                session.setPrivateKeyFile(advModel.sshKyeFile);
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

    private void createDebugProcess(
            @NotNull ExecutionEnvironment environment,
            @Nullable Callback callback,
            @NotNull SparkBatchJobSubmissionState submissionState,
            boolean isDriver,
            @NotNull Subscriber<? super String> debugSessionSubscriber,
            @NotNull Phaser debugPhaser,
            String remoteHost,
            int remotePort,
            String logUrl,
            final CredentialsProvider credentialsProvider
    ) {
        SparkBatchDebugSession session = getDebugSession();
        ReplaySubject<SimpleEntry<String, Key>> debugProcessConsole = ReplaySubject.create();

        if (session == null) {
            return;
        }

        debugPhaser.register();

        Observable.create((Observable.OnSubscribe<SimpleEntry<String, Key>>) ob -> {
            try {
                List<Subscription> debugProcessSubscriptions = new ArrayList<>();

                // Create a new state for Executor debugging process
                SparkBatchJobSubmissionState state = isDriver ? submissionState :
                        (SparkBatchJobSubmissionState) environment.getState();

                // Forward port
                session.forwardToRemotePort(remoteHost, remotePort);
                int localPort = session.getForwardedLocalPort(remoteHost, remotePort);

                Observable<SimpleEntry<String, Key>> debugProcessOb = JobUtils.createYarnLogObservable(
                                                            credentialsProvider,
                                                            logUrl,
                                                            "stderr",
                                                            SparkBatchJobDebuggerRunner.this.getLogReadBlockSize())
                        .scan(new SimpleEntry<>((String) null, ProcessOutputTypes.STDERR),
                                (lastLineKeyPair, line) -> {
                                    Matcher logMatcher = simpleLogPattern.matcher(line);

                                    if (logMatcher.matches()) {
                                        String logType = logMatcher.group(1);
                                        Key logKey = (logType.equals("ERROR") || logType.equals("WARN")) ?
                                                ProcessOutputTypes.STDERR :
                                                ProcessOutputTypes.STDOUT;

                                        return new SimpleEntry<>(line, logKey);
                                    }

                                    return new SimpleEntry<>(line, lastLineKeyPair.getValue());
                                })
                        .filter(lineKeyPair -> lineKeyPair.getKey() != null);

                if (isDriver) {
                    PublishSubject<String> closeSubject = PublishSubject.create();
                    PublishSubject<String> openSubject = PublishSubject.create();

                    debugProcessOb = debugProcessOb.share();

                    Subscription executorFindingSubscription = debugProcessOb
                            .map(lineKeyPair -> {
                                String line = lineKeyPair.getKey();

                                if (line.matches("^YARN executor launch context:$")) {
                                    openSubject.onNext("YARN executor launch");
                                }

                                if (line.matches("^={5,}$")) {
                                    closeSubject.onNext("=====");
                                }

                                return line;
                            })
                            .window(openSubject, s -> closeSubject)
                            .subscribe(executorLunchContextOb -> executorLunchContextOb
                                    .subscribe(line -> {
                                        Matcher matcher = executorLogUrlPattern.matcher(line);

                                        if (matcher.matches()) {

                                            String host = matcher.group(1);
                                            String containerId = matcher.group(2);

                                            try {
                                                URI baseUri = new URI(logUrl);
                                                String driverLogUrl = baseUri.resolve(String.format(
                                                        "/yarnui/%s/node/containerlogs/%s/livy", host, containerId)).toString();

                                                createDebugProcess( environment,
                                                                    callback,
                                                                    submissionState,
                                                                    false,
                                                                    debugSessionSubscriber,
                                                                    debugPhaser,
                                                                    host,
                                                                    6006,
                                                                    driverLogUrl,
                                                                    credentialsProvider);
                                            } catch (URISyntaxException ignore) { }
                                        }
                                    })
                            );

                    debugProcessSubscriptions.add(executorFindingSubscription);
                }

                Subscription processLogSubscription = debugProcessOb.subscribe(ob::onNext, ob::onError);
                debugProcessSubscriptions.add(processLogSubscription);

                if (state != null) {
                    // Set the debug connection to localhost and local forwarded port to the state
                    state.setRemoteConnection(
                            new RemoteConnection(true, "localhost", Integer.toString(localPort), false));

                    // Execute with attaching to JVM through local forwarded port
                    SparkBatchJobDebuggerRunner.super.execute(buildChildEnvironment(environment, remoteHost, isDriver),
                            (runContentDescriptor) -> {
                                ProcessHandler handler = runContentDescriptor.getProcessHandler();

                                if (handler != null) {
                                    debugProcessConsole.subscribe(lineKeyPair -> handler.notifyTextAvailable(
                                                                                    lineKeyPair.getKey() + "\n",
                                                                                    lineKeyPair.getValue()));

                                    handler.addProcessListener(new ProcessAdapter() {
                                        @Override
                                        public void processTerminated(ProcessEvent processEvent) {
                                            debugProcessSubscriptions.forEach(Subscription::unsubscribe);
                                            ob.onCompleted();
                                            debugPhaser.forceTermination();
                                        }
                                    });
                                } else {
                                    ob.onCompleted();
                                }

                                if (callback != null) {
                                    callback.processStarted(runContentDescriptor);
                                }
                            }, state);
                }
            } catch (Exception e) {
                ob.onError(e);
            }
        })
        .subscribeOn(Schedulers.io())
        .subscribe(debugProcessConsole::onNext, debugSessionSubscriber::onError, debugPhaser::arriveAndDeregister);
    }

    protected int getLogReadBlockSize() {
        return 4096;
    }
}
