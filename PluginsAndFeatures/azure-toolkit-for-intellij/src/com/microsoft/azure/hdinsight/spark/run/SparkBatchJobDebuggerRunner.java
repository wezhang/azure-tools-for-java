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

import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.intellij.debugger.impl.GenericDebuggerRunner;
import com.intellij.debugger.impl.GenericDebuggerRunnerSettings;
import com.intellij.execution.ExecutionException;
import com.intellij.execution.ExecutionResult;
import com.intellij.execution.Executor;
import com.intellij.execution.configurations.*;
import com.intellij.execution.process.ProcessAdapter;
import com.intellij.execution.process.ProcessEvent;
import com.intellij.execution.runners.ExecutionEnvironment;
import com.intellij.execution.runners.ExecutionEnvironmentBuilder;
import com.intellij.execution.runners.ProgramRunner;
import com.intellij.execution.ui.RunContentDescriptor;
//import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Key;
import com.intellij.xdebugger.XDebuggerManager;
import com.intellij.xdebugger.impl.XDebuggerManagerImpl;
import com.microsoft.azure.hdinsight.common.HDInsightUtil;
import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azure.hdinsight.spark.common.SparkBatchDebugSession;
import com.microsoft.azure.hdinsight.spark.common.SparkBatchRemoteDebugJob;
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitAdvancedConfigModel;
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitAdvancedConfigModel.SSHAuthType;
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitModel;
import com.microsoft.azure.hdinsight.spark.run.configuration.RemoteDebugRunConfiguration;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import com.microsoft.intellij.rxjava.IdeaSchedulers;
import org.apache.commons.lang3.StringUtils;
import rx.Observable;
import rx.Subscriber;
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

import static com.microsoft.azure.hdinsight.common.MessageInfoType.Info;
import static com.microsoft.azure.hdinsight.common.MessageInfoType.Error;

public class SparkBatchJobDebuggerRunner extends GenericDebuggerRunner {
    private static final Key<String> DebugTargetKey = new Key<>("debug-target");
    private static final Key<String> ProfileNameKey = new Key<>("profile-name");
    private static final String DebugDriver = "driver";
    private static final String DebugExecutor = "executor";

    private SparkBatchDebugSession debugSession;
    private boolean isAppInsightEnabled = true;
    private Phaser debugProcessPhaser;
    private SparkBatchRemoteDebugJob debugJob;

//    @NotNull
//    private IdeaSchedulers schedulers;

//    private SparkBatchJobRemoteDebugProcess remoteDebugProcess;

    // Control Log subject
//    @NotNull
//    private PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject = PublishSubject.create();

    // More complex pattern, please use grok
    private final Pattern simpleLogPattern = Pattern.compile("\\d{1,2}[/-]\\d{1,2}[/-]\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2} (INFO|WARN|ERROR) .*", Pattern.DOTALL);

    private int jdbLocalPort;

    @Nullable
    private ExecutionException remoteDebugProcessError;
    private SparkBatchJobDebugProcessHandler debugProcessHandler;

//    public SparkBatchJobDebuggerRunner(@Nullable Project project) {
//        this.schedulers = new IdeaSchedulers(project);
//    }

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
        boolean isDebugEnabled = Optional.of((RemoteDebugRunConfiguration) profile)
                .map(RemoteDebugRunConfiguration::getSubmitModel)
                .map(SparkSubmitModel::getAdvancedConfigModel)
                .map(advModel -> advModel.enableRemoteDebug &&
                        StringUtils.isNotEmpty(advModel.sshUserName) &&
                        StringUtils.isNotEmpty(advModel.sshAuthType == SSHAuthType.UsePassword ?
                                advModel.sshPassword :
                                advModel.sshKeyFile.getPath()))
                .orElse(false);

        // Only support debug now, will enable run in future
        return SparkBatchJobDebugExecutor.EXECUTOR_ID.equals(executorId) &&
                profile instanceof RemoteDebugRunConfiguration &&
                isDebugEnabled;
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
//        final SparkBatchJobDebugProcessHandler handler = Optional.ofNullable(state.execute(environment.getExecutor(), this))
//                .map(ExecutionResult::getProcessHandler)
//                .map(SparkBatchJobDebugProcessHandler.class::cast)
//                .orElse(null);
//
//        if (handler == null) {
//            return null;
//        }
//
//        try {
//            SparkBatchDebugJobJdbPortForwardedEvent jdbReadyEvent = (SparkBatchDebugJobJdbPortForwardedEvent) handler
//                    .getDebugProcess()
//                    .getEventSubject()
//                    .observeOn(Schedulers.io())
//                    .filter(SparkBatchDebugJobJdbPortForwardedEvent.class::isInstance)
//                    .toBlocking()
//                    .first();
//
//            RemoteConnection connection = new RemoteConnection(
//                    true,
//                    "localhost",
//                    jdbReadyEvent.getLocalJdbForwardedPort()
//                            .map(port -> Integer.toString(port))
//                            .orElseThrow(() -> new ExecutionException("Can't open local forwarded port, " +
//                                    "check the SSH connection with manually login please.")),
//                    false);
//
//            return attachVirtualMachine(state, environment, connection, false);
//        } catch (Exception e) {
//            throw new ExecutionException(e);
//        }
//
//    }

    private void ctrlMessage(@NotNull MessageInfoType type, @NotNull String message) {
        getCtrlSubject().onNext(new SimpleImmutableEntry<>(type, message));
    }

    /**
     *
     * Running in Event dispatch thread
     *
     * @param environment
     * @param callback
     * @param state
     * @throws ExecutionException
     */
    @Override
    protected void execute(ExecutionEnvironment environment, Callback callback, RunProfileState state) throws ExecutionException {
        SparkBatchJobSubmissionState submissionState = (SparkBatchJobSubmissionState) state;
        ExecutionResult result = state.execute(environment.getExecutor(), this);
        SparkSubmitModel submitModel = submissionState.getSubmitModel();

        debugProcessHandler = Optional.ofNullable(result)
                .map(ExecutionResult::getProcessHandler)
                .map(SparkBatchJobDebugProcessHandler.class::cast)
                .orElse(null);

        if (debugProcessHandler == null) {
            return;
        }

//        XDebuggerManager.getInstance(submitModel.getProject()).getDebugSession(result.getExecutionConsole())
//                .getUI().getComponent().setVisible(true);


        try {
            debugProcessHandler.getDebugProcess()
                    .getEventSubject()
                    .observeOn(Schedulers.io())
                    .filter(SparkBatchDebugJobJdbPortForwardedEvent.class::isInstance)
                    .map(SparkBatchDebugJobJdbPortForwardedEvent.class::cast)
                    .subscribe(jdbReadyEvent -> {
                        jdbReadyEvent.getLocalJdbForwardedPort()
                                .ifPresent(localPort -> {
                                    setJdbLocalPort(localPort);

                                    try {
                                        // Set the debug connection to localhost and local forwarded port to the state
                                        submissionState.setRemoteConnection(
                                                new RemoteConnection(true, "localhost", Integer.toString(localPort), false));

                                        boolean isDriver = true;
//
                                        super.execute(
                                                buildChildEnvironment(environment, jdbReadyEvent.getRemoteHost().orElse("unknown"), isDriver),
                                                (runContentDescriptor) -> {
                                                    SparkBatchJobDebugProcessHandler ihandler = (SparkBatchJobDebugProcessHandler)
                                                            runContentDescriptor.getProcessHandler();
//
                                                    if (ihandler != null) {
                                                        // Debugger is setup rightly
//                                                    //                                    debugProcessConsole.subscribe(lineKeyPair ->
//                                                    //                                            handler.notifyTextAvailable(lineKeyPair.getKey() + "\n", lineKeyPair.getValue()));
//                                                    //                                    debugProcessConsole.unsubscribeOn(Schedulers.immediate());
//
                                                        ihandler.addProcessListener(new ProcessAdapter() {
                                                            @Override
                                                            public void processTerminated(ProcessEvent processEvent) {
                                                                // JDB Debugger is stopped, tell the debug process
//                                                            stopConsoleLogSubject.onNext("stop");
//                                                                debugPhaser.arriveAndDeregister();
                                                            }
                                                        });
//                                                } else {
//                                                    ob.onCompleted();
                                                    }

                                                    if (callback != null) {
                                                        callback.processStarted(runContentDescriptor);
                                                    }

//                                                debugPhaser.arriveAndDeregister();

//                                                    ob.onCompleted();
                                                },
                                                submissionState);
                                    } catch (ExecutionException e) {
                                        throw new UncheckedExecutionException(e);
                                    }
                                });

                    });

        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    //    @Override
    protected void executeOld(@NotNull ExecutionEnvironment environment, @Nullable Callback callback, @NotNull RunProfileState state) throws ExecutionException {
        SparkBatchJobSubmissionState submissionState = (SparkBatchJobSubmissionState) state;
        SparkSubmitModel submitModel = submissionState.getSubmitModel();
//        Project project = submitModel.getProject();

        submissionState.checkSubmissionParameter();

        if (!submitModel.getAdvancedConfigModel().enableRemoteDebug) {
            throw new ExecutionException("The Spark remote debugging is not enabled, please config it at Run/Debug configuration -> Remotely Run in Cluster -> Advanced configuration.");
        }

        // Reset the debug process Phaser
        debugProcessPhaser = new Phaser(1);

        try {
            // Create Driver debug process
            createDebugProcess(
                    environment,
                    callback,
                    submissionState,
                    true,
                    debugProcessPhaser);

            ctrlMessage(Info, "Spark Job Driver debugging process created.");
            ctrlMessage(Info, "Spark Job Driver debugging is starting.");

            // Await for all debug processes finish
            debugProcessPhaser.arriveAndAwaitAdvance();

            ctrlMessage(Info, "Debugging Spark batch job in cluster is done.");
            postAppInsightDebugSuccessEvent(environment.getExecutor(), submissionState);
        } catch (Exception throwable) {
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

            ctrlMessage(Error, "Spark batch Job remote debug failed, got exception: " + errorMessage);

            postAppInsightDebugErrorEvent(environment.getExecutor(), submissionState, errorMessage);
            debugProcessPhaser.forceTermination();

            throw new ExecutionException(throwable);
        } finally {
            stopDebugJob();
        }

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


    /*
     * Create a debug process, if it's a Driver process, the following Executor processes will be created
     */
    private void createDebugProcess(@NotNull ExecutionEnvironment environment,
                                    @Nullable Callback callback,
                                    @NotNull SparkBatchJobSubmissionState submissionState,
                                    boolean isDriver,
                                    @NotNull Phaser debugPhaser
    ) {
        SparkSubmitModel submitModel = submissionState.getSubmitModel();
//        Project project = submitModel.getProject();
        ReplaySubject<SimpleEntry<String, Key>> debugProcessConsole = ReplaySubject.create();
//
//        try {
////            remoteDebugProcess = new SparkBatchJobRemoteDebugProcess(project, submitModel, ctrlSubject);
////            remoteDebugProcess.start();
//
//            processHandler = null;// new SparkBatchJobDebugProcessHandler(project, submitModel);
////            processHandler = new SparkBatchJobDebugProcessHandler(project, submitModel);
////            super.execute(buildChildEnvironment(environment, "", isDriver), callback, submissionState);
//
//            // new debug process start
//            debugPhaser.register();
//        } catch (ExecutionException ex) {
//            remoteDebugProcessError = ex;
//            return;
//        }

        // Handle process ctrl events
        processHandler.getDebugProcess()
                .getEventSubject()
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
                                (SparkBatchJobExecutorCreatedEvent) event, environment, callback, debugPhaser);
                    }

                    return Observable.empty();
                })
                .subscribeOn(Schedulers.io())
                .subscribe(
                        log -> getCtrlSubject().onNext(new SimpleImmutableEntry<>(Info, log)),
                        err -> getCtrlSubject().onError(err)
                );
    }

    private Observable<String> handleDebugJobExecutorCreated(SparkBatchJobExecutorCreatedEvent event,
                                               ExecutionEnvironment environment,
                                               Callback callback,
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
                        debugPhaser);
            } catch (InterruptedException ignore) {
            } catch (ExecutionException | UnknownServiceException ex) {
                ob.onError(ex);
            }

            ob.onCompleted();
        });
    }

    private Observable<String> handleDebugJobJdbPortForwarded(SparkBatchDebugJobJdbPortForwardedEvent event,
                                                              ExecutionEnvironment environment,
                                                              Callback callback,
                                                              SparkBatchJobSubmissionState submissionState,
                                                              String host,
                                                              boolean isDriver,
                                                              Phaser debugPhaser) {

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
