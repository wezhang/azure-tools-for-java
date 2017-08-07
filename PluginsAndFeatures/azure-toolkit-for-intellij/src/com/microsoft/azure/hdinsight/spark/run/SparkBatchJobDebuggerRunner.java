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
import com.intellij.execution.configurations.*;
import com.intellij.execution.executors.DefaultDebugExecutor;
import com.intellij.execution.process.ProcessAdapter;
import com.intellij.execution.process.ProcessEvent;
import com.intellij.execution.process.ProcessHandler;
import com.intellij.execution.process.ProcessOutputTypes;
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
import java.net.UnknownServiceException;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.concurrent.Phaser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static rx.exceptions.Exceptions.propagate;

public class SparkBatchJobDebuggerRunner extends GenericDebuggerRunner {
    private static final Key<String> DebugTargetKey = new Key<>("debug-target");
    private static final Key<String> ProfileNameKey = new Key<>("profile-name");
    private static final String DebugDriver = "driver";
    private static final String DebugExecutor = "executor";

    private SparkBatchDebugSession debugSession;
    private boolean isAppInsightEnabled = true;
    private Phaser debugProcessPhaser;
    private SparkBatchRemoteDebugJob debugJob;

    // More complex pattern, please use grok
    private final Pattern simpleLogPattern = Pattern.compile("\\d{1,2}[/-]\\d{1,2}[/-]\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2} (INFO|WARN|ERROR) .*", Pattern.DOTALL);
    private final Pattern executorLogUrlPattern = Pattern.compile("^\\s+SPARK_LOG_URL_STDERR -> https?://([^:]+):?\\d*/node/containerlogs/(container.*)/livy/stderr.*");

    public void setDebugJob(SparkBatchRemoteDebugJob debugJob) {
        this.debugJob = debugJob;
    }

    public SparkBatchRemoteDebugJob getDebugJob() {
        return debugJob;
    }

    public SparkBatchDebugSession getDebugSession() {
        return debugSession;
    }

    public void setDebugSession(SparkBatchDebugSession debugSession) {
        this.debugSession = debugSession;
    }

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
                createDebugJobSession(submitModel).subscribe(debugJobClusterPair-> {
                    final SparkBatchRemoteDebugJob remoteDebugJob = debugJobClusterPair.getKey();
                    final IClusterDetail clusterDetail = debugJobClusterPair.getValue();
                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

                    try {
                        jobStatusMgmt.resetJobStateManager();
                        jobStatusMgmt.setJobRunningState(true);

                        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
                                clusterDetail.getHttpUserName(), clusterDetail.getHttpPassword()));

                        String driverHost = remoteDebugJob.getSparkDriverHost();
                        int driverDebugPort = remoteDebugJob.getSparkDriverDebuggingPort();
                        String logUrl = remoteDebugJob.getSparkJobDriverLogUrl(
                                remoteDebugJob.getConnectUri(), remoteDebugJob.getBatchId());

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
                    } catch (Exception ex) {
                        ob.onError(ex);
                    }


                    ob.onNext("Info: Spark Job Driver debugging started.");

                    Subscription livyLogSubscription = submitModel
                            .jobLogObservable(remoteDebugJob.getBatchId(), clusterDetail)
                            .subscribeOn(Schedulers.io())
                            .subscribe();

                    // Await for all debug processes finish
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

    /*
     * Build a child environment with specified host and type
     */
    private ExecutionEnvironment buildChildEnvironment(@NotNull ExecutionEnvironment parentEnv,
                                                       String host,
                                                       boolean isDriver) {
        String savedProfileName = parentEnv.getUserData(ProfileNameKey);
        String originProfileName = savedProfileName == null ? parentEnv.getRunProfile().getName() : savedProfileName;

        RunConfiguration newRunConfiguration = ((RunConfiguration) parentEnv.getRunProfile()).clone();
        newRunConfiguration.setName(originProfileName + " [" + (isDriver ? "Driver " : "Executor ") + host + "]");

        ExecutionEnvironment childEnv = new ExecutionEnvironmentBuilder(parentEnv).runProfile(newRunConfiguration)
                .build();

        childEnv.putUserData(DebugTargetKey, isDriver ? DebugDriver : DebugExecutor);
        childEnv.putUserData(ProfileNameKey, originProfileName);

        return childEnv;
    }

    /*
     * Create a Debug Spark Job session with building, deploying and submitting
     */
    private Single<SimpleEntry<SparkBatchRemoteDebugJob, IClusterDetail>> createDebugJobSession(
                                                                            @NotNull SparkSubmitModel submitModel) {
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
                        SparkBatchRemoteDebugJob remoteDebugJob =
                                submitModel.tryToCreateBatchSparkDebugJob(selectedClusterDetail);
                        SparkBatchDebugSession session = createSparkBatchDebugSession(
                                selectedClusterDetail.getConnectionUrl(), submitModel.getAdvancedConfigModel()).open();

                        setDebugSession(session);
                        setDebugJob(remoteDebugJob);
                        return new SimpleEntry<>(remoteDebugJob, selectedClusterDetail);
                    } catch (Exception e) {
                        HDInsightUtil.setJobRunningStatus(submitModel.getProject(), false);
                        throw propagate(e);
                    }
                });
    }

    private void postAppInsightDebugSuccessEvent() {
        if (!isAppInsightEnabled) {
            return;
        }

        Map<String, String> postEventProperty = new HashMap<>();

        postEventProperty.put("IsSubmitSucceed", "true");
        AppInsightsClient.create(HDInsightBundle.message("SparkRunConfigDebugButtonClick"), null, postEventProperty);
    }

    private void postAppInsightDebugErrorEvent(String errorMessage) {
        if (!isAppInsightEnabled) {
            return;
        }

        Map<String, String> postEventProperty = new HashMap<>();

        postEventProperty.put("IsSubmitSucceed", "false");
        postEventProperty.put("SubmitFailedReason", HDInsightUtil.normalizeTelemetryMessage(errorMessage));
        AppInsightsClient.create(HDInsightBundle.message("SparkRunConfigDebugButtonClick"), null, postEventProperty);
    }

    /*
     * Stop the debug job
     */
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

    /*
     * Create a Spark Batch Job Debug Session with SSH certification
     */
    private SparkBatchDebugSession createSparkBatchDebugSession(
            String connectionUrl, SparkSubmitAdvancedConfigModel advModel) throws SparkJobException, JSchException {
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

    /*
     * Create a debug process, if it's a Driver process, the following Executor processes will be created
     */
    private void createDebugProcess(@NotNull ExecutionEnvironment environment,
                                    @Nullable Callback callback,
                                    @NotNull SparkBatchJobSubmissionState submissionState,
                                    boolean isDriver,
                                    @NotNull Subscriber<? super String> debugSessionSubscriber,
                                    @NotNull Phaser debugPhaser,
                                    String remoteHost,
                                    int remotePort,
                                    String logUrl,
                                    final CredentialsProvider credentialsProvider ) {
        SparkBatchDebugSession session = getDebugSession();
        ReplaySubject<SimpleEntry<String, Key>> debugProcessConsole = ReplaySubject.create();

        if (session == null) {
            return;
        }

        // new debug process start
        debugPhaser.register();

        Observable.create((Observable.OnSubscribe<SimpleEntry<String, Key>>) ob -> {
            try {
                // Store all debug process subscription to clean up after finished
                List<Subscription> debugProcessSubscriptions = new ArrayList<>();
                Observable<SimpleEntry<String, Key>> debugProcessOb =
                        createDebugProcessObservable(logUrl, credentialsProvider);

                if (isDriver) {
                    debugProcessOb = debugProcessOb.share();

                    Subscription executorFindingSubscription = matchedExecutorFromDebugProcessObservable(debugProcessOb)
                            .subscribe(hostContainerPair -> {
                                String host = hostContainerPair.getKey();
                                String containerId = hostContainerPair.getValue();

                                try {
                                    String executorLogUrl = new URI(logUrl).resolve(String.format(
                                            "/yarnui/%s/node/containerlogs/%s/livy", host, containerId)).toString();
                                    int executorJdbPort = getDebugJob().getYarnContainerJDBListenPort(executorLogUrl);

                                    // Create a new state for Executor debugging process
                                    SparkBatchJobSubmissionState newExecutorState =
                                            (SparkBatchJobSubmissionState) environment.getState();

                                    if (newExecutorState == null) {
                                        throw new ExecutionException("Can't get Executor debug state.");
                                    }

                                    // create debug process for the Spark job executor
                                    createDebugProcess( environment,
                                                        callback,
                                                        newExecutorState,
                                                        false,
                                                        debugSessionSubscriber,
                                                        debugPhaser,
                                                        host,
                                                        executorJdbPort,
                                                        executorLogUrl,
                                                        credentialsProvider);
                                } catch (URISyntaxException ignore) {
                                } catch (ExecutionException | UnknownServiceException ex) {
                                    ob.onError(ex);
                                }
                            });

                    debugProcessSubscriptions.add(executorFindingSubscription);
                }

                Subscription processLogSubscription = debugProcessOb.subscribe(ob::onNext, ob::onError);
                debugProcessSubscriptions.add(processLogSubscription);

                // Forward port
                int localPort = session.forwardToRemotePort(remoteHost, remotePort)
                                       .getForwardedLocalPort(remoteHost, remotePort);

                // Set the debug connection to localhost and local forwarded port to the state
                submissionState.setRemoteConnection(
                        new RemoteConnection(true, "localhost", Integer.toString(localPort), false));

                // Execute with attaching to JVM through local forwarded port
                SparkBatchJobDebuggerRunner.super.execute(buildChildEnvironment(environment, remoteHost, isDriver),
                        (runContentDescriptor) -> {
                            ProcessHandler handler = runContentDescriptor.getProcessHandler();

                            if (handler != null) {
                                // Debugger is setup rightly
                                debugProcessConsole.subscribe(lineKeyPair ->
                                        handler.notifyTextAvailable( lineKeyPair.getKey() + "\n", lineKeyPair.getValue()));

                                handler.addProcessListener(new ProcessAdapter() {
                                    @Override
                                    public void processTerminated(ProcessEvent processEvent) {
                                        // Debug is stopped, do clean up
                                        debugProcessSubscriptions.forEach(Subscription::unsubscribe);
                                        ob.onCompleted();

                                        // force all debug process to stop
                                        debugPhaser.forceTermination();
                                    }
                                });
                            } else {
                                ob.onCompleted();
                            }

                            if (callback != null) {
                                callback.processStarted(runContentDescriptor);
                            }
                        }, submissionState);
            } catch (Exception e) {
                ob.onError(e);
            }
        })
        .subscribeOn(Schedulers.io())
        .subscribe(debugProcessConsole::onNext, debugSessionSubscriber::onError, debugPhaser::arriveAndDeregister);
    }

    /*
     * Create an Observable for a debug process, the Yarn log 'stderr' will be considered as the events
     * with its type key.
     */
    private Observable<SimpleEntry<String, Key>> createDebugProcessObservable(
                                                    String logUrl,
                                                    final CredentialsProvider credentialsProvider) {
        return JobUtils.createYarnLogObservable(
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
    }

    /**
     * To match Executor lunch content from debug process Observable
     *
     * @param debugProcessOb the debug process Observable to match
     * @return matched Executor Observable, the event is SimpleEntry with host, containerId pair
     */
    private Observable<SimpleEntry<String, String>> matchedExecutorFromDebugProcessObservable(
                                                        Observable<SimpleEntry<String, Key>> debugProcessOb) {
        PublishSubject<String> closeSubject = PublishSubject.create();
        PublishSubject<String> openSubject = PublishSubject.create();

        return debugProcessOb
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
                .flatMap(executorLunchContextOb -> executorLunchContextOb
                                                    .map(executorLogUrlPattern::matcher)
                                                    .filter(Matcher::matches)
                                                    .map(matcher -> new SimpleEntry<>(matcher.group(1), matcher.group(2)))
                );
    }

    protected int getLogReadBlockSize() {
        return 4096;
    }
}
