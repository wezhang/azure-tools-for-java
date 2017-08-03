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
import com.intellij.execution.ExecutionResult;
import com.intellij.execution.Executor;
import com.intellij.execution.configurations.*;
import com.intellij.execution.executors.DefaultDebugExecutor;
import com.intellij.execution.process.ProcessEvent;
import com.intellij.execution.process.ProcessHandler;
import com.intellij.execution.process.ProcessListener;
import com.intellij.execution.process.ProcessOutputTypes;
import com.intellij.execution.runners.ExecutionEnvironment;
import com.intellij.execution.runners.ExecutionEnvironmentBuilder;
import com.intellij.execution.ui.RunContentDescriptor;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Key;
import com.jcraft.jsch.JSchException;
import com.microsoft.azure.hdinsight.common.ClusterManagerEx;
import com.microsoft.azure.hdinsight.common.HDInsightUtil;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.sdk.common.HDIException;
import com.microsoft.azure.hdinsight.spark.common.*;
import com.microsoft.azure.hdinsight.spark.jobs.JobUtils;
import com.microsoft.azure.hdinsight.spark.run.configuration.RemoteDebugRunConfiguration;
import com.microsoft.azuretools.telemetry.AppInsightsClient;
import com.microsoft.intellij.hdinsight.messages.HDInsightBundle;
import com.microsoft.tooling.msservices.helpers.collections.ListChangeListener;
import com.microsoft.tooling.msservices.helpers.collections.ListChangedAction;
import com.microsoft.tooling.msservices.helpers.collections.ListChangedEvent;
import com.microsoft.tooling.msservices.helpers.collections.ObservableList;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static rx.exceptions.Exceptions.propagate;

public class SparkBatchJobDebuggerRunner extends GenericDebuggerRunner {
    enum DebugAction {
        STOP
    }

    class DebugProcess {

    }

    public static final Key<String> DebugTargetKey = new Key<>("debug-target");
    public static final String DebugDriver = "driver";
    public static final String DebugExecutor = "executor";

    private ProcessHandler remoteDebuggerProcessHandler;
    private PublishSubject<DebugAction> actionSubject = PublishSubject.create();
    private PublishSubject<String> toolwindowInfoSubject = PublishSubject.create();
    private PublishSubject<String> toolwindowErrSubject = PublishSubject.create();

    // More complex pattern, please use grok
    private Pattern simpleLogPattern = Pattern.compile("\\d{1,2}[/-]\\d{1,2}[/-]\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2} (INFO|WARN|ERROR) .*", Pattern.DOTALL);

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
    public void onProcessStarted(RunnerSettings settings, ExecutionResult executionResult) {
        super.onProcessStarted(settings, executionResult);

        remoteDebuggerProcessHandler = executionResult.getProcessHandler();
    }

    public Optional<ProcessHandler> getRemoteDebuggerProcessHandler() {
        return Optional.ofNullable(remoteDebuggerProcessHandler);
    }

    private ExecutionEnvironment buildChildEnvironment(@NotNull ExecutionEnvironment environment, String name) {
        RunConfiguration driverRunConfiguration = ((RunConfiguration) environment.getRunProfile()).clone();
        driverRunConfiguration.setName(environment.getRunProfile().getName() + " [" + name + "]");

        return new ExecutionEnvironmentBuilder(environment).runProfile(driverRunConfiguration).build();
    }

    private ExecutionEnvironment buildDriverEnvironment(@NotNull ExecutionEnvironment environment, String ipaddr) {
        ExecutionEnvironment driverEnv = buildChildEnvironment(environment, "Driver " + ipaddr);
        driverEnv.putUserData(DebugTargetKey, DebugDriver);

        return driverEnv;
    }

    private ExecutionEnvironment buildExecutorEnvironment(@NotNull ExecutionEnvironment environment, String ipaddr) {
        ExecutionEnvironment executorEnv = buildChildEnvironment(environment, "Executor " + ipaddr);
        executorEnv.putUserData(DebugTargetKey, DebugExecutor);

        return executorEnv;
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
                                submitModel.getAdvancedConfigModel());

                        setDebugSession(session);
                        return new SimpleEntry<>(remoteDebugJob, clusterDetail);
                    } catch (Exception ex) {
                        throw propagate(ex);
                    }
                });
    }

    private SparkBatchDebugSession debugSession;
    private boolean isAppInsightEnabled = true;
    //private ObservableList<Subscription> debugProcessSubscriptions = new ObservableList<>();
    final private Phaser debugProcessPhaser = new Phaser(1);

    public SparkBatchDebugSession getDebugSession() {
        return debugSession;
    }

    public void setDebugSession(SparkBatchDebugSession debugSession) {
        this.debugSession = debugSession;
    }


    private void postAppInsightDebugSuccedEvent() {
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
        SparkSubmissionParameter submissionParameter = submitModel.getSubmissionParameter();
        Map<String, String> postEventProperty = new HashMap<>();
        Project project = submitModel.getProject();


        Observable
        .create((Observable.OnSubscribe<String>) ob -> {
            buildDebugSession(submitModel)
                    .subscribe(debugJobClusterPair-> {
                        final SparkBatchRemoteDebugJob remoteDebugJob = debugJobClusterPair.getKey();
                        final IClusterDetail clusterDetail = debugJobClusterPair.getValue();
                        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        String driverHost = null;
                        int driverDebugPort = 0;
                        String logUrl = null;

                        try {
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
                                driverHost,
                                driverDebugPort,
                                logUrl,
                                credentialsProvider
                        );

                        ob.onNext("Info: Spark Job Driver debugging started.");

                        Subscription livyLogSubscription = submitModel.jobLogObservable(
                                                                            remoteDebugJob.getBatchId(), clusterDetail)
                                .subscribeOn(Schedulers.computation())
                                .subscribe();

                        debugProcessPhaser.arriveAndAwaitAdvance();
                        ob.onCompleted();

                        livyLogSubscription.unsubscribe();
                    });
        })
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
                    if (getDebugSession() != null) {
                        getDebugSession().close();

                        postAppInsightDebugSuccedEvent();
                    }

                    // Spark Job is done
                    HDInsightUtil.showInfoOnSubmissionMessageWindow(
                            submitModel.getProject(), "Info : Debugging Spark batch job in cluster is done.");
                    HDInsightUtil.setJobRunningStatus(project, false);
                }
        );

//
//
//
//        submitModel
//                .buildArtifactObservable(submissionParameter.getArtifactName())
//                .flatMap(artifact -> {
//                    IClusterDetail clusterDetail = Optional.ofNullable(submitModel.getSelectedClusterDetail())
//                            .orElseGet(() -> ClusterManagerEx.getInstance()
//                                    .getClusterDetailsWithoutAsync(true, submitModel.getProject())
//                                    .stream()
//                                    .filter(cluster -> cluster.getName().equals(submissionParameter.getClusterName()))
//                                    .findFirst()
//                                    .orElse(null));
//
//                    return Single.just(new Pair<>(artifact, clusterDetail));
//                })
//                .flatMap(pair -> submitModel.deployArtifactObservable(pair.first(), pair.second())
//                                                  .subscribeOn(Schedulers.io()))
//                .map((selectedClusterDetail) -> {
//                    // Create Batch Spark Debug Job
//                    try {
//                        return new Pair<>(
//                                submitModel.tryToCreateBatchSparkDebugJob(selectedClusterDetail),
//                                selectedClusterDetail);
//                    } catch (Exception e) {
//                        HDInsightUtil.setJobRunningStatus(submitModel.getProject(), false);
//                        throw propagate(e);
//                    }
//                })
//                .flatMap(pair -> {
//                    SparkBatchRemoteDebugJob remoteDebugJob = pair.first();
//                    IClusterDetail clusterDetail = pair.second();
//
//                    try {
//                        SparkBatchDebugSession session = createDebugSession(clusterDetail.getConnectionUrl(),
//                                submitModel.getAdvancedConfigModel());
//
//                        return startDebuggerObservable(submitModel.getProject(),
//                                                       environment,
//                                                       callback,
//                                                       clusterDetail,
//                                                       submissionState,
//                                                       session,
//                                                       remoteDebugJob)
//                                .doOnEach(forwardedSession ->
//                                        // Handle STOP action with subscribing actionSubject for STOP event
//                                        actionSubject.filter((action) -> action.equals(DebugAction.STOP))
//                                                .subscribe(action -> {
//                                                    try {
//                                                        HDInsightUtil.showInfoOnSubmissionMessageWindow(
//                                                                submitModel.getProject(),
//                                                                "Info: Spark batch debugging job stop, job is killed");
//
//                                                        remoteDebugJob.killBatchJob();
//                                                    } catch (IOException ex) {
//                                                        HDInsightUtil.showErrorMessageOnSubmissionMessageWindow(
//                                                                submitModel.getProject(),
//                                                                "Error : Failed to kill Spark batch debugging job, " +
//                                                                        "got exception " + ex);
//                                                    }
//                                                }))
//                                .subscribeOn(Schedulers.computation())
//                                .zipWith( // Block with getting the job log from cluster
//                                        submitModel.jobLogObservable(
//                                                remoteDebugJob.getBatchId(), clusterDetail)
//                                                .subscribeOn(Schedulers.computation()),
//                                        (debugSession, ignore) -> debugSession)
//                                .doOnError(err -> {
//                                    try {
//                                        HDInsightUtil.showErrorMessageOnSubmissionMessageWindow(
//                                                submitModel.getProject(),
//                                                "Error : Spark batch debugging job is killed, got exception " + err);
//
//                                        remoteDebugJob.killBatchJob();
//                                        HDInsightUtil.setJobRunningStatus(submitModel.getProject(), false);
//                                    } catch (IOException ignore) {
//                                    }
//                                });
//                    } catch (Exception ex) {
//                        throw propagate(ex);
//                    }
//                })
//                .subscribe(
//                        sparkBatchDebugSession -> {
//                            // Spark Job is done
//                            HDInsightUtil.showInfoOnSubmissionMessageWindow(
//                                    submitModel.getProject(),
//                                    "Info : Debugging Spark batch job in cluster is done.");
//
//                            sparkBatchDebugSession.close();
//
//                            HDInsightUtil.setJobRunningStatus(submitModel.getProject(), false);
//
//                            postEventProperty.put("IsSubmitSucceed", "true");
//                            AppInsightsClient.create(
//                                    HDInsightBundle.message("SparkRunConfigDebugButtonClick"), null,
//                                    postEventProperty);
//                        },
//                        (throwable) -> {
//                            // set the running flag to false
//                            HDInsightUtil.setJobRunningStatus(submitModel.getProject(), false);
//
//                            String errorMessage;
//
//                            if (throwable instanceof CompositeException) {
//                                CompositeException exceptions = (CompositeException) throwable;
//
//                                errorMessage = exceptions.getExceptions().stream()
//                                        .map(Throwable::getMessage)
//                                        .collect(Collectors.joining("; "));
//                            } else {
//                                errorMessage = throwable.getMessage();
//                            }
//
//                            HDInsightUtil.showErrorMessageOnSubmissionMessageWindow(
//                                    submitModel.getProject(),
//                                    "Error : Spark batch Job remote debug failed, got exception: " + errorMessage);
//
//                            postEventProperty.put("IsSubmitSucceed", "false");
//                            postEventProperty.put("SubmitFailedReason", HDInsightUtil.normalizeTelemetryMessage(errorMessage));
//                            AppInsightsClient.create(
//                                    HDInsightBundle.message("SparkRunConfigDebugButtonClick"),
//                                    null,
//                                    postEventProperty);
//                        });
    }

    /**
     * Stop the runner by sending STOP event to all subscribers
     */
    public void performStopAction() {
        actionSubject.onNext(DebugAction.STOP);
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

        String sshServer = null;
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

    /**
     * Create SSH port forwarding session for debugging
     *
     * @param connectionUrl the HDInsight connection URL, such as: https://spkdbg.azurehdinsight.net/batch
     * @param submitModel the Spark submit model with advanced setting
     * @param remoteDebugJob the remote Spark job which is listening a port for debugging
     * @return Spark batch debug session and local forwarded port pair
     * @throws URISyntaxException connection URL is invalid
     * @throws JSchException SSH connection exception
     * @throws IOException networking exception
     * @throws SparkSubmitAdvancedConfigModel.UnknownSSHAuthTypeException invalid SSH authentication type
     */
//    protected SimpleEntry<SparkBatchDebugSession, Integer> createSshPortForwardForDebugSession(
//            Project project,
//            IClusterDetail clusterDetail,
//            //SparkSubmitModel submitModel,
//            SparkBatchRemoteDebugJob remoteDebugJob,
//            SparkBatchDebugSession session
//    )
//            throws URISyntaxException, JSchException, IOException, HDIException {
//        String driverHost = remoteDebugJob.getSparkDriverHost();
//        int driverDebugPort = remoteDebugJob.getSparkDriverDebuggingPort();
//
//        HDInsightUtil.showInfoOnSubmissionMessageWindow(
//                project,
//                String.format("Info : Remote Spark batch job is listening on %s:%d",
//                              driverHost, driverDebugPort));
//
//        session.open().forwardToRemotePort(driverHost, driverDebugPort);
//
//        int localPort = session.getForwardedLocalPort(driverHost, driverDebugPort);
//
//        String driverLogUrl = remoteDebugJob.getSparkJobDriverLogUrl(remoteDebugJob.getConnectUri(), remoteDebugJob.getBatchId());
//
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(
//                AuthScope.ANY,
//                new UsernamePasswordCredentials(clusterDetail.getHttpUserName(), clusterDetail.getHttpPassword()));
//
//        session.setLogSubscription(JobUtils.createYarnLogObservable(
//                                        credentialsProvider,
//                                        driverLogUrl,
//                                        "stderr",
//                                        getLogReadBlockSize())
//                .scan(new SimpleEntry<>(null, ProcessOutputTypes.STDERR), (lastLineKeyPair, line) -> {
//                    Matcher logMatcher = simpleLogPattern.matcher(line);
//
//                    if (logMatcher.matches()) {
//                        String logType = logMatcher.group(1);
//                        Key logKey = (logType.equals("ERROR") || logType.equals("WARN")) ?
//                                ProcessOutputTypes.STDERR :
//                                ProcessOutputTypes.STDOUT;
//
//                        return new SimpleEntry<>(line, logKey);
//                    }
//
//                    return new SimpleEntry<>(line, lastLineKeyPair.getValue());
//                })
//                .filter(lineKeyPair -> lineKeyPair.getKey() != null)
//                .subscribe(
//                        lineKeyPair -> remoteDebuggerProcessHandler.ifPresent(processHandler ->
//                                processHandler.notifyTextAvailable(
//                                        lineKeyPair.getKey() + "\n", lineKeyPair.getValue())),
//                        error -> HDInsightUtil.showInfoOnSubmissionMessageWindow(
//                                project, error.getMessage())));
//
//        HDInsightUtil.showInfoOnSubmissionMessageWindow(
//                project,
//                String.format("Info : Local port %d is forwarded to %s:%d for Spark job driver debugging",
//                        localPort, driverHost, driverDebugPort));
//
//        return new SimpleEntry<>(session, localPort);
//    }

    /**
     * Start Spark batch job remote debugging
     *
     * @param environment ID of the {@link Executor} with which the user is trying to run the configuration.
     * @param callback callback when debugger is prepared
     * @param submissionState the submission state from run configuration
     * @return a single Observable with SparkBatchDebugSession instance which is done
     */
//    protected Single<SparkBatchDebugSession> startDebuggerObservable(
//            @NotNull Project project,
//            @NotNull ExecutionEnvironment environment,
//            @Nullable Callback callback,
//            @NotNull IClusterDetail clusterDetail,
//            @NotNull SparkBatchJobSubmissionState submissionState,
//            SparkBatchDebugSession session,
//            @NotNull SparkBatchRemoteDebugJob remoteDebugJob) {
//        //SparkSubmitModel submitModel = submissionState.getSubmitModel();
//
//        return Single.fromEmitter(em -> {
//            SimpleEntry<SparkBatchDebugSession, Integer> sessionPortPair = null;
//
//            try {
//                // Create SSH port forwarding session for debugging
//                sessionPortPair =
//                        createSshPortForwardForDebugSession(project, clusterDetail, remoteDebugJob, session);
//
//                doDebug(environment, callback, submissionState, Integer.toString(sessionPortPair.getValue()));
//
//                em.onSuccess(sessionPortPair.getKey());
//            } catch (Exception ex) {
//                if (sessionPortPair != null) {
//                    sessionPortPair.getKey().close();
//                }
//
//                em.onError(ex);
//            }
//        });
//    }

    private Subscription createDebugProcess(
            @NotNull ExecutionEnvironment environment,
            @Nullable Callback callback,
            @NotNull SparkBatchJobSubmissionState submissionState,
            boolean isDriver,
            @NotNull Subscriber<? super String> debugSessionSubscriber,
            String remoteHost,
            int remotePort,
            String logUrl,
            final CredentialsProvider credentialsProvider
    ) {
        SparkBatchDebugSession session = getDebugSession();

        if (session == null) {
            return null;
        }

        debugProcessPhaser.register();

        Subscription subscription = Observable.create((Observable.OnSubscribe<SimpleEntry<String, Key>>) ob -> {
            try {
                // Forwarding port
                session.open().forwardToRemotePort(remoteHost, remotePort);

                int localPort = session.getForwardedLocalPort(remoteHost, remotePort);

//                String driverLogUrl = baseUri.resolve("/yarnui/" + remoteHost + "/node/containerlogs/" + containerId
//                        + "/livy").toString();

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

                debugProcessOb.subscribe(ob::onNext, ob::onError);

                // Set the debug connection to localhost and local forwarded port to the state
                submissionState.setRemoteConnection(
                        new RemoteConnection(true, "localhost", Integer.toString(localPort), false));

                // Execute with attaching to JVM through local forwarded port
                SparkBatchJobDebuggerRunner.super.execute(isDriver ? buildDriverEnvironment(environment, remoteHost) :
                                                                     buildExecutorEnvironment(environment, remoteHost),
                        (runContentDescriptor) -> {
                    ProcessHandler handler = runContentDescriptor.getProcessHandler();

                    if (handler != null) {
                        handler.addProcessListener(new ProcessListener() {
                            @Override
                            public void startNotified(ProcessEvent processEvent) { }

                            @Override
                            public void processTerminated(ProcessEvent processEvent) {
                                ob.onCompleted();
                            }

                            @Override
                            public void processWillTerminate(ProcessEvent processEvent, boolean b) { }

                            @Override
                            public void onTextAvailable(ProcessEvent processEvent, Key key) { }
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
        .subscribe(
                lineKeyPair -> getRemoteDebuggerProcessHandler().ifPresent(processHandler ->
                        processHandler.notifyTextAvailable( lineKeyPair.getKey() + "\n", lineKeyPair.getValue())),
                debugSessionSubscriber::onError,
                debugProcessPhaser::arriveAndDeregister
        );

        return subscription;
    }

//    public void doDebug(@NotNull ExecutionEnvironment environment,
//                        @Nullable Callback callback,
//                        @NotNull SparkBatchJobSubmissionState submissionState,
//                        @NotNull String address) throws ExecutionException {
//        // Set the debug connection to localhost and local forwarded port to the state
//        submissionState.setRemoteConnection(
//                new RemoteConnection(true, "localhost", address, false));
//
//        // Execute with attaching to JVM through local forwarded port
//        super.execute(environment, callback, submissionState);
//    }

    protected int getLogReadBlockSize() {
        return 4096;
    }
}
