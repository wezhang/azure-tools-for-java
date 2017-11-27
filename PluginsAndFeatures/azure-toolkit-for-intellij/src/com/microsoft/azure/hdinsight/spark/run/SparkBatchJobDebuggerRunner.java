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

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.intellij.debugger.engine.JavaDebugProcess;
import com.intellij.debugger.impl.GenericDebuggerRunner;
import com.intellij.debugger.impl.GenericDebuggerRunnerSettings;
import com.intellij.execution.DefaultExecutionResult;
import com.intellij.execution.ExecutionException;
import com.intellij.execution.ExecutionResult;
import com.intellij.execution.Executor;
import com.intellij.execution.configurations.*;
import com.intellij.execution.process.ProcessAdapter;
import com.intellij.execution.process.ProcessEvent;
import com.intellij.execution.runners.ExecutionEnvironment;
import com.intellij.execution.runners.ExecutionEnvironmentBuilder;
import com.intellij.execution.ui.ConsoleView;
import com.intellij.execution.ui.ConsoleViewContentType;
import com.intellij.execution.ui.RunContentDescriptor;
import com.intellij.openapi.application.ApplicationManager;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Key;
import com.intellij.xdebugger.XDebugProcess;
import com.intellij.xdebugger.XDebugProcessStarter;
import com.intellij.xdebugger.XDebugSession;
import com.intellij.xdebugger.XDebuggerManager;
import com.intellij.xdebugger.impl.XDebugSessionImpl;
import com.intellij.xdebugger.impl.ui.XDebugSessionTab;
import com.microsoft.azure.hdinsight.common.HDInsightUtil;
import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azure.hdinsight.spark.common.*;
import com.microsoft.azure.hdinsight.spark.run.configuration.RemoteDebugRunConfiguration;
import com.microsoft.azure.hdinsight.spark.ui.SparkJobLogConsoleView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import rx.*;
import rx.exceptions.CompositeException;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

import java.io.IOException;
import java.net.UnknownServiceException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SparkBatchJobDebuggerRunner extends GenericDebuggerRunner {
    private static final Key<String> DebugTargetKey = new Key<>("debug-target");
    private static final Key<String> ProfileNameKey = new Key<>("profile-name");
    private static final String DebugDriver = "driver";
    private static final String DebugExecutor = "executor";

    private SparkBatchDebugSession debugSession;
    private boolean isAppInsightEnabled = true;
    private Phaser debugProcessPhaser;
    private SparkBatchRemoteDebugJob debugJob;

//    private SparkBatchJobRemoteDebugProcess remoteDebugProcess;

    // Control Log subject
//    @NotNull
//    private PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject = PublishSubject.create();

    // More complex pattern, please use grok
    private final Pattern simpleLogPattern = Pattern.compile("\\d{1,2}[/-]\\d{1,2}[/-]\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2} (INFO|WARN|ERROR) .*", Pattern.DOTALL);

    public SparkBatchJobDebugProcessHandler getProcessHandler() {
        return processHandler;
    }

    private SparkBatchJobDebugProcessHandler processHandler;

    public void setDebugJob(SparkBatchRemoteDebugJob debugJob) {
        this.debugJob = debugJob;
    }

    public Optional<SparkBatchRemoteDebugJob> getDebugJob() {
        return Optional.ofNullable(debugJob);
    }

    public Optional<SparkBatchDebugSession> getDebugSession() {
        return Optional.ofNullable(debugSession);
    }

    public void setDebugSession(SparkBatchDebugSession debugSession) {
        this.debugSession = debugSession;
    }

    @Override
    public boolean canRun(@NotNull String executorId, @NotNull RunProfile profile) {
        // Only support debug now, will enable run in future
        return SparkBatchJobDebugExecutor.EXECUTOR_ID.equals(executorId) && profile instanceof RemoteDebugRunConfiguration;
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

//    @Nullable
//    @Override
//    protected RunContentDescriptor createContentDescriptor(@NotNull RunProfileState state, @NotNull ExecutionEnvironment environment) throws ExecutionException {
//        return getJdbLocalPort()
//                .map(localPort -> new RemoteConnection(true, "localhost", Integer.toString(localPort), false))
//                .map(connection -> {
//                    try {
//                        return attachVirtualMachine(state, environment, connection, false);
//                    } catch (ExecutionException e) {
//                        throw new UncheckedExecutionException(e);
//                    }
//                })
//                .orElse(null);
//    }

    @Override
    protected void execute(@NotNull ExecutionEnvironment environment, @Nullable Callback callback, @NotNull RunProfileState state) throws ExecutionException {
        SparkBatchJobSubmissionState submissionState = (SparkBatchJobSubmissionState) state;
        SparkSubmitModel submitModel = submissionState.getSubmitModel();
        Project project = submitModel.getProject();
        IdeSchedulers schedulers = new IdeaSchedulers(project);

        submissionState.checkSubmissionParameter();

        if (!submitModel.getAdvancedConfigModel().enableRemoteDebug) {
            throw new ExecutionException("The Spark remote debugging is not enabled, please config it at Run/Debug configuration -> Remotely Run in Cluster -> Advanced configuration.");
        }

        // JobStatusManager jobStatusMgmt = HDInsightUtil.getSparkSubmissionToolWindowManager(project)
        //         .getJobStatusManager();

        // Reset the debug process Phaser
        debugProcessPhaser = new Phaser(1);

        Observable.create((Observable.OnSubscribe<String>) ob -> {
//                createDebugJobSession(submitModel).subscribe(debugJobClusterPair -> {
//                    final SparkBatchRemoteDebugJob remoteDebugJob = debugJobClusterPair.getKey();
//                    final IClusterDetail clusterDetail = debugJobClusterPair.getValue();
//                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

                    try {
//                        jobStatusMgmt.resetJobStateManager();
//                        jobStatusMgmt.setJobRunningState(true);

//                        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
//                                clusterDetail.getHttpUserName(), clusterDetail.getHttpPassword()));

//                        String driverHost = remoteDebugJob.getSparkDriverHost();
//                        int driverDebugPort = remoteDebugJob.getSparkDriverDebuggingPort();
//                        String logUrl = remoteDebugJob.getSparkJobDriverLogUrl(
//                                remoteDebugJob.getConnectUri(), remoteDebugJob.getBatchId());

                        ApplicationManager.getApplication().invokeAndWait(() -> {
                        // Create Driver debug process
                            createDebugProcess(
                                    environment,
                                    callback,
                                    submissionState,
                                    true,
                                    ob,
                                    debugProcessPhaser);
                            ob.onNext("Info: Spark Job Driver debugging process created.");
//                                driverHost,
//                                driverDebugPort,
//                                logUrl,
//                                credentialsProvider
                                });
                    } catch (Exception ex) {
                        ob.onError(ex);
                    }


                    ob.onNext("Info: Spark Job Driver debugging is starting.");

//                    Subscription livyLogSubscription = submitModel
//                            .jobLogObservable(remoteDebugJob.getBatchId(), clusterDetail)
//                            .subscribeOn(Schedulers.io())
//                            .subscribe();

                    // Await for all debug processes finish
                    debugProcessPhaser.arriveAndAwaitAdvance();
                    ob.onCompleted();

//                    livyLogSubscription.unsubscribe();
//                }, ob::onError))
                })
                .subscribeOn(Schedulers.io())
                .subscribe(
                        info -> HDInsightUtil.showInfoOnSubmissionMessageWindow(project, info),
                        throwable -> {
//                            jobStatusMgmt.setJobKilled();
                            stopDebugJob();

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

                            postAppInsightDebugErrorEvent(environment.getExecutor(), submissionState, errorMessage);
                            debugProcessPhaser.forceTermination();
//                            HDInsightUtil.setJobRunningStatus(project, false);
                        },
                        () -> {
//                            jobStatusMgmt.setJobKilled();
                            stopDebugJob();

                            // Spark Job is done
                            HDInsightUtil.showInfoOnSubmissionMessageWindow(
                                    submitModel.getProject(), "Info : Debugging Spark batch job in cluster is done.");

                            postAppInsightDebugSuccessEvent(environment.getExecutor(), submissionState);
//                            HDInsightUtil.setJobRunningStatus(project, false);
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
//    private Single<SimpleEntry<SparkBatchRemoteDebugJob, IClusterDetail>> createDebugJobSession(
//                                                                            @NotNull SparkSubmitModel submitModel) {
//        SparkSubmissionParameter submissionParameter = submitModel.getSubmissionParameter();
//        String selectedClusterName = submissionParameter.getClusterName();
//
//        return submitModel
//                .buildArtifactObservable(submissionParameter.getArtifactName())
//                .flatMap(artifact -> Single.create((SingleSubscriber<? super IClusterDetail> ob) -> {
//                    try {
//                        IClusterDetail clusterDetail = ClusterManagerEx.getInstance()
//                                .getClusterDetailByName(selectedClusterName)
//                                .orElseThrow(() -> new HDIException(
//                                        "No cluster name matched selection: " + selectedClusterName));
//
//                        String jobArtifactUri = JobUtils.uploadFileToCluster(
//                                clusterDetail,
//                                submitModel.getArtifactPath(artifact.getName())
//                                           .orElseThrow(() -> new SparkJobException("Can't find jar path to upload")),
//                                HDInsightUtil.getToolWindowMessageSubject());
//
//                        submissionParameter.setFilePath(jobArtifactUri);
//
//                        ob.onSuccess(clusterDetail);
//                    } catch (Exception e) {
//                        ob.onError(e);
//                    }
//                }).subscribeOn(Schedulers.io()))
//                .map((selectedClusterDetail) -> {
//                    // Create Batch Spark Debug Job
//                    try {
//                        SparkBatchRemoteDebugJob remoteDebugJob =
//                                submitModel.tryToCreateBatchSparkDebugJob(selectedClusterDetail);
//                        setDebugJob(remoteDebugJob);
//
//                        SparkBatchDebugSession session = createSparkBatchDebugSession(
//                                selectedClusterDetail.getConnectionUrl(), submitModel.getAdvancedConfigModel()).open();
//                        setDebugSession(session);
//
//                        return new SimpleEntry<>(remoteDebugJob, selectedClusterDetail);
//                    } catch (Exception e) {
//                        HDInsightUtil.setJobRunningStatus(submitModel.getProject(), false);
//                        throw propagate(e);
//                    }
//                });
//    }

    private void postAppInsightDebugSuccessEvent(@NotNull Executor executor, @NotNull SparkBatchJobSubmissionState state) {
        if (!isAppInsightEnabled) {
            return;
        }

        state.createAppInsightEvent(executor, new HashMap<String, String>() {{
            put("IsSubmitSucceed", "true");
        }});
    }

    private void postAppInsightDebugErrorEvent(@NotNull Executor executor,
                                               @NotNull SparkBatchJobSubmissionState state,
                                               @NotNull String errorMessage) {
        if (!isAppInsightEnabled) {
            return;
        }

        state.createAppInsightEvent(executor, new HashMap<String, String>() {{
            put("IsSubmitSucceed", "false");
            put("SubmitFailedReason", HDInsightUtil.normalizeTelemetryMessage(errorMessage));
        }});
    }

    /*
     * Stop the debug job
     */
    private void stopDebugJob() {
        getDebugSession().ifPresent(SparkBatchDebugSession::close);
        getDebugJob().ifPresent(sparkBatchRemoteDebugJob -> {
            try {
                sparkBatchRemoteDebugJob.killBatchJob();
            } catch (IOException ignore) { }
        });
    }

//    /**
//     * Get SSH Host from the HDInsight connection URL
//     *
//     * @param connectionUrl the HDInsight connection URL, such as: https://spkdbg.azurehdinsight.net/batch
//     * @return SSH host
//     * @throws URISyntaxException connection URL is invalid
//     */
//    protected String getSshHost(String connectionUrl) throws URISyntaxException {
//        URI connectUri = new URI(connectionUrl);
//        String segs[] = connectUri.getHost().split("\\.");
//        segs[0] = segs[0].concat("-ssh");
//        return StringUtils.join(segs, ".");
//    }

//    /*
//     * Create a Spark Batch Job Debug Session with SSH certification
//     */
//    public SparkBatchDebugSession createSparkBatchDebugSession(
//            String connectionUrl, SparkSubmitAdvancedConfigModel advModel) throws SparkJobException, JSchException {
//        if (advModel == null) {
//            throw new SparkSubmitAdvancedConfigModel.NotAdvancedConfig("SSH authentication not set");
//        }
//
//        String sshServer;
//
//        try {
//            sshServer = getSshHost(connectionUrl);
//        } catch (URISyntaxException e) {
//            throw new SparkJobException("Connection URL is not valid: " + connectionUrl);
//        }
//
//        SparkBatchDebugSession session = SparkBatchDebugSession.factory(sshServer, advModel.sshUserName);
//
//        switch (advModel.sshAuthType) {
//            case UseKeyFile:
//                session.setPrivateKeyFile(advModel.sshKeyFile);
//                break;
//            case UsePassword:
//                session.setPassword(advModel.sshPassword);
//                break;
//            default:
//                throw new SparkSubmitAdvancedConfigModel.UnknownSSHAuthTypeException(
//                        "Unknown SSH authentication type: " + advModel.sshAuthType.name());
//        }
//
//        return session;
//    }

    /*
     * Create a debug process, if it's a Driver process, the following Executor processes will be created
     */
    private void createDebugProcess(SparkBatchRemoteDebugJob remoteDebugJob,
                                    @NotNull ExecutionEnvironment environment,
                                    @Nullable Callback callback,
                                    @NotNull SparkBatchJobSubmissionState submissionState,
                                    boolean isDriver,
                                    @NotNull Subscriber<? super String> debugSessionSubscriber,
                                    @NotNull Phaser debugPhaser
//                                    String remoteHost,
//                                    int remotePort,
//                                    String logUrl,
//                                    final CredentialsProvider credentialsProvider
    ) {
        SparkSubmitModel submitModel = submissionState.getSubmitModel();
        Project project = submitModel.getProject();
        ReplaySubject<SimpleEntry<String, Key>> debugProcessConsole = ReplaySubject.create();

        try {
//            remoteDebugProcess = new SparkBatchJobRemoteDebugProcess(project, submitModel, ctrlSubject);
//            remoteDebugProcess.start();

            processHandler = new SparkBatchJobDebugProcessHandler(project, submitModel);
//            super.execute(buildChildEnvironment(environment, "", isDriver), callback, submissionState);

            // new debug process start
            debugPhaser.register();
        } catch (ExecutionException ex) {
            remoteDebugProcessError = ex;
            return;
        }

//        remoteDebugProcess
        processHandler.getDebugProcess()
                .getEventSubject()
//                .subscribeOn(Schedulers.io())
                .flatMap(event -> {
                    if (event instanceof SparkBatchDebugJobJdbPortForwardedEvent) {
                        return ((SparkBatchDebugJobJdbPortForwardedEvent) event).getRemoteHost()
                                .map(host -> handleDebugJobJdbPortForwarded(
                                       (SparkBatchDebugJobJdbPortForwardedEvent) event,
                                        environment,
                                        callback,
                                        submissionState,
                                        host,
                                        isDriver,
                                        debugPhaser))
                                .orElse(Observable.empty());
                    } else if (event instanceof SparkBatchJobExecutorCreatedEvent) {
                        return handleDebugJobExecutorCreated(
                                (SparkBatchJobExecutorCreatedEvent) event, environment, callback, debugSessionSubscriber, debugPhaser);
                    }

                    return Observable.empty();
//                    } else if (event instanceof SparkBatchJobSubmittedEvent)
                })
//                .subscribeOn(Schedulers.io())
                .subscribe(
                        log -> getCtrlSubject().onNext(new SimpleImmutableEntry<>(MessageInfoType.Info, log)),
                        err -> getCtrlSubject().onError(err)
                );

//        Observable.create((Observable.OnSubscribe<SimpleEntry<String, Key>>) ob -> {
//            try {
                // Stop subject to send stop event to debug process
//                PublishSubject<Object> stopConsoleLogSubject = PublishSubject.create();
//                Observable<SimpleEntry<String, Key>> debugProcessOb =
//                        createDebugProcessObservable(logUrl, credentialsProvider, stopConsoleLogSubject);

//                if (isDriver) {
//                    debugProcessOb = debugProcessOb.share();
//
//                    matchExecutorFromDebugProcessObservable(debugProcessOb)
//                            .subscribe(hostContainerPair -> {
//                                String host = hostContainerPair.getKey();
//                                String containerId = hostContainerPair.getValue();
//
//                                try {
//                                    String executorLogUrl = new URI(logUrl).resolve(String.format(
//                                            "/yarnui/%s/node/containerlogs/%s/livy", host, containerId)).toString();
//
//                                    int executorJdbPort = 0;
//                                    int retries = 0;
//                                    SparkBatchRemoteDebugJob debugJob = getDebugJob().orElseThrow(() ->
//                                            new ExecutionException("No debug job created"));
//
//                                    // Retry in case the Executor JVM not bring up
//                                    while (executorJdbPort == 0) {
//                                        try {
//                                            executorJdbPort = debugJob.getYarnContainerJDBListenPort(executorLogUrl);
//                                        } catch (UnknownServiceException ex) {
//                                            if (retries++ > 3) {
//                                                throw ex;
//                                            }
//
//                                            Thread.sleep(5000);
//                                        }
//                                    }
//
//                                    // Create a new state for Executor debugging process
//                                    SparkBatchJobSubmissionState newExecutorState =
//                                            (SparkBatchJobSubmissionState) environment.getState();
//
//                                    if (newExecutorState == null) {
//                                        throw new ExecutionException("Can't get Executor debug state.");
//                                    }
//
//                                    // create debug process for the Spark job executor
//                                    createDebugProcess( environment,
//                                                        callback,
//                                                        newExecutorState,
//                                                        false,
//                                                        debugSessionSubscriber,
//                                                        debugPhaser,
//                                                        host,
//                                                        executorJdbPort,
//                                                        executorLogUrl,
//                                                        credentialsProvider);
//                                } catch (URISyntaxException | InterruptedException ignore) {
//                                } catch (ExecutionException | UnknownServiceException ex) {
//                                    ob.onError(ex);
//                                }
//                            });
//                }

//                debugProcessOb.subscribe(ob::onNext, ob::onError, () -> {
//                    ob.onCompleted();
//
//                    // force all debug process to stop
//                    debugPhaser.forceTermination();
//                    stopConsoleLogSubject.onCompleted();
//                });

                // Execute with attaching to JVM through local forwarded port
//                SparkBatchJobDebuggerRunner.super.execute(buildChildEnvironment(environment, remoteHost, isDriver),
//                SparkBatchJobDebuggerRunner.super.execute(buildChildEnvironment(environment, "driver", isDriver),
//                        (runContentDescriptor) -> {
//                            SparkBatchJobRunProcessHandler handler = (SparkBatchJobRunProcessHandler)
//                                    runContentDescriptor.getProcessHandler();
//
//                            SparkBatchJobRemoteDebugProcess debugProcess = (SparkBatchJobRemoteDebugProcess)
//                                    handler.getProcess();
//
//                            debugProcess.getSparkJob()
//                                    .map(job -> (SparkBatchRemoteDebugJob) job)
//                                    .ifPresent(job -> {
//                                        try {
//                                            String remoteHost = job.getSparkDriverHost();
//                                            int remotePort = job.getSparkDriverDebuggingPort();
//                                            // Forward port
//                                            SparkBatchDebugSession session = getDebugSession().orElse(null);
//                                            if (session == null) {
//                                                return;
//                                            }
//
//                                            int localPort = session.forwardToRemotePort(remoteHost, remotePort)
//                                                    .getForwardedLocalPort(remoteHost, remotePort);
//
//                                            // Set the debug connection to localhost and local forwarded port to the state
//                                            submissionState.setRemoteConnection(
//                                                    new RemoteConnection(true, "localhost", Integer.toString(localPort), false));
//
//                                        } catch (Exception e) {
//                                            e.printStackTrace();
//                                        }
//                                    });
//
//
//                            if (handler != null) {
//                                // Debugger is setup rightly
//                                debugProcessConsole.subscribe(lineKeyPair ->
//                                        handler.notifyTextAvailable(lineKeyPair.getKey() + "\n", lineKeyPair.getValue()));
//                                debugProcessConsole.unsubscribeOn(Schedulers.immediate());
//
//                                handler.addProcessListener(new ProcessAdapter() {
//                                    @Override
//                                    public void processTerminated(ProcessEvent processEvent) {
//                                        // JDB Debugger is stopped, tell the debug process
//                                        stopConsoleLogSubject.onNext("stop");
//                                    }
//                                });
//                            } else {
//                                ob.onCompleted();
//                            }
//
//                            if (callback != null) {
//                                callback.processStarted(runContentDescriptor);
//                            }
//                        }, submissionState);
//            } catch (Exception e) {
//                ob.onError(e);
//            }
//        })
//                .subscribeOn(Schedulers.io())
//                .subscribe(debugProcessConsole::onNext, debugSessionSubscriber::onError, debugPhaser::arriveAndDeregister);
    }

    private Observable<String> handleDebugJobExecutorCreated(SparkBatchJobExecutorCreatedEvent event,
                                               ExecutionEnvironment environment,
                                               Callback callback,
                                               Subscriber<? super String> debugSessionSubscriber,
                                               Phaser debugPhaser) {
        return Observable.create(ob -> {
            try {
                String executorLogUrl = event.getJob().getConnectUri().resolve(String.format(
                        "/yarnui/%s/node/containerlogs/%s/livy", event.getHost(), event.getContainerId())).toString();

                int executorJdbPort = 0;
                int retries = 0;
                SparkBatchRemoteDebugJob debugJob = getDebugJob().orElseThrow(() ->
                        new ExecutionException("No debug job created"));

                // Retry in case the Executor JVM not bring up
                while (executorJdbPort == 0) {
                    try {
                        executorJdbPort = debugJob.getYarnContainerJDBListenPort(executorLogUrl);
                    } catch (UnknownServiceException ex) {
                        if (retries++ > 3) {
                            throw ex;
                        }

                        Thread.sleep(5000);
                    }
                }

                // Create a new state for Executor debugging process
                SparkBatchJobSubmissionState newExecutorState =
                        (SparkBatchJobSubmissionState) environment.getState();

                if (newExecutorState == null) {
                    throw new ExecutionException("Can't get Executor debug state.");
                }

                // create debug process for the Spark job executor
                createDebugProcess(environment,
                        callback,
                        newExecutorState,
                        false,
                        debugSessionSubscriber,
                        debugPhaser);
            } catch (InterruptedException ignore) {
            } catch (ExecutionException | UnknownServiceException ex) {
                ob.onError(ex);
            }

            ob.onCompleted();
        });
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

    private Observable<String> handleDebugJobJdbPortForwarded(SparkBatchDebugJobJdbPortForwardedEvent event,
                                                              ExecutionEnvironment environment,
                                                              Callback callback,
                                                              SparkBatchJobSubmissionState submissionState,
                                                              String host,
                                                              boolean isDriver,
                                                              Phaser debugPhaser) {

//            SparkBatchDebugSession session = createSparkBatchDebugSession(
//                    job.getConnectUri().toString(),
//                    submitModel.getAdvancedConfigModel())
//                    .open();
//
        return Observable.create((Subscriber<? super String> ob) -> {
                    setDebugSession(event.getDebugSession());

                    event.getLocalJdbForwardedPort()
                            .ifPresent(localPort -> {
                                setJdbLocalPort(localPort);

                                try {
                                    // Set the debug connection to localhost and local forwarded port to the state
                                    submissionState.setRemoteConnection(
                                            new RemoteConnection(true, "localhost", Integer.toString(localPort), false));
//
//                                    super.execute(buildChildEnvironment(environment, "", isDriver), callback, submissionState);
                                    super.execute(
                                            buildChildEnvironment(environment, host, isDriver),
                                            (runContentDescriptor) -> {
                                                SparkBatchJobDebugProcessHandler handler = (SparkBatchJobDebugProcessHandler)
                                                        runContentDescriptor.getProcessHandler();
//
                                                if (handler != null) {
                                                    // Debugger is setup rightly
//                                                    //                                    debugProcessConsole.subscribe(lineKeyPair ->
//                                                    //                                            handler.notifyTextAvailable(lineKeyPair.getKey() + "\n", lineKeyPair.getValue()));
//                                                    //                                    debugProcessConsole.unsubscribeOn(Schedulers.immediate());
//
                                                    handler.addProcessListener(new ProcessAdapter() {
                                                        @Override
                                                        public void processTerminated(ProcessEvent processEvent) {
                                                            // JDB Debugger is stopped, tell the debug process
//                                                            stopConsoleLogSubject.onNext("stop");
                                                            debugPhaser.arriveAndDeregister();
                                                        }
                                                    });
//                                                } else {
//                                                    ob.onCompleted();
                                                }

                                                if (callback != null) {
                                                    callback.processStarted(runContentDescriptor);
                                                }

//                                                debugPhaser.arriveAndDeregister();

                                                ob.onCompleted();
                                            },
                                            submissionState);
                                } catch (ExecutionException e) {
                                    ob.onError(e);
                                }
                            });

                }).subscribeOn(Schedulers.io());

//                    event.getRemoteHost()
//                            .ifPresent(host -> {
//                                try {
//                                    super.execute(
//                                            buildChildEnvironment(environment, host, isDriver),
//                                            //                            (runContentDescriptor) -> {
//                                            //                                SparkBatchJobRunProcessHandler handler = (SparkBatchJobRunProcessHandler)
//                                            //                                        runContentDescriptor.getProcessHandler();
//                                            //
//                                            //                                if (handler != null) {
//                                            //                                    // Debugger is setup rightly
//                                            ////                                    debugProcessConsole.subscribe(lineKeyPair ->
//                                            ////                                            handler.notifyTextAvailable(lineKeyPair.getKey() + "\n", lineKeyPair.getValue()));
//                                            ////                                    debugProcessConsole.unsubscribeOn(Schedulers.immediate());
//                                            //
//                                            ////                                    handler.addProcessListener(new ProcessAdapter() {
//                                            ////                                        @Override
//                                            ////                                        public void processTerminated(ProcessEvent processEvent) {
//                                            ////                                            // JDB Debugger is stopped, tell the debug process
//                                            ////                                            stopConsoleLogSubject.onNext("stop");
//                                            ////                                        }
//                                            ////                                    });
//                                            //                                } else {
//                                            //                                    ob.onCompleted();
//                                            //                                }
//                                            //
//                                            //                                if (callback != null) {
//                                            //                                    callback.processStarted(runContentDescriptor);
//                                            //                                }
//                                            //                            },
//                                            callback,
//                                            submissionState);
//                                } catch (ExecutionException e) {
//                                    throw propagate(e);
//                                }
//                            });
//                });

//
//            String remoteHost = job.getSparkDriverHost();
//            int remotePort = job.getSparkDriverDebuggingPort();
//
//            setJdbLocalPort(session
//                    .forwardToRemotePort(remoteHost, remotePort)
//                    .getForwardedLocalPort(remoteHost, remotePort));
    }
    /*
     * Create an Observable for a debug process, the Yarn log 'stderr' will be considered as the events
     * with its type key.
     */
//    private Observable<SimpleEntry<String, Key>> createDebugProcessObservable(
//                                                                String logUrl,
//                                                                final CredentialsProvider credentialsProvider,
//                                                                Observable<Object> stopSubject) {
//        return JobUtils.createYarnLogObservable(
//                credentialsProvider,
//                stopSubject,
//                logUrl,
//                "stderr",
//                SparkBatchJobDebuggerRunner.this.getLogReadBlockSize())
//                .scan(new SimpleEntry<>((String) null, ProcessOutputTypes.STDERR),
//                        (lastLineKeyPair, line) -> {
//                            Matcher logMatcher = simpleLogPattern.matcher(line);
//
//                            if (logMatcher.matches()) {
//                                String logType = logMatcher.group(1);
//                                Key logKey = (logType.equals("ERROR") || logType.equals("WARN")) ?
//                                        ProcessOutputTypes.STDERR :
//                                        ProcessOutputTypes.STDOUT;
//
//                                return new SimpleEntry<>(line, logKey);
//                            }
//
//                            return new SimpleEntry<>(line, lastLineKeyPair.getValue());
//                        })
//                .filter(lineKeyPair -> lineKeyPair.getKey() != null);
//    }

    /**
     * To match Executor lunch content from debug process Observable
     *
     * @param debugProcessOb the debug process Observable to match
     * @return matched Executor Observable, the event is SimpleEntry with host, containerId pair
     */
//    private Observable<SimpleEntry<String, String>> matchExecutorFromDebugProcessObservable(
//                                                        Observable<SimpleEntry<String, Key>> debugProcessOb) {
//        PublishSubject<String> closeSubject = PublishSubject.create();
//        PublishSubject<String> openSubject = PublishSubject.create();
//
//        return debugProcessOb
//                .map(lineKeyPair -> {
//                    String line = lineKeyPair.getKey();
//
//                    if (line.matches("^YARN executor launch context:$")) {
//                        openSubject.onNext("YARN executor launch");
//                    }
//
//                    if (line.matches("^={5,}$")) {
//                        closeSubject.onNext("=====");
//                    }
//
//                    return line;
//                })
//                .window(openSubject, s -> closeSubject)
//                .flatMap(executorLunchContextOb -> executorLunchContextOb
//                                                    .map(executorLogUrlPattern::matcher)
//                                                    .filter(Matcher::matches)
//                                                    .map(matcher -> new SimpleEntry<>(matcher.group(1), matcher.group(2)))
//                );
//    }

    protected int getLogReadBlockSize() {
        return 4096;
    }

//    public Optional<SparkBatchJobRemoteDebugProcess> getRemoteDebugProcess() {
//        return Optional.ofNullable(remoteDebugProcess);
//    }

    public void setJdbLocalPort(int jdbLocalPort) {
        this.jdbLocalPort = jdbLocalPort;
    }

    public Optional<Integer> getJdbLocalPort() {
        return Optional.of(jdbLocalPort)
                .filter(port -> port > 0);
    }

    public Optional<ExecutionException> getRemoteDebugProcessError() {
        return Optional.ofNullable(remoteDebugProcessError);
    }

    @NotNull
    public PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> getCtrlSubject() {
        return processHandler.getCtrlSubject();
    }

}
