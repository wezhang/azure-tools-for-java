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
import com.intellij.debugger.impl.GenericDebuggerRunner;
import com.intellij.debugger.impl.GenericDebuggerRunnerSettings;
import com.intellij.execution.DefaultExecutionResult;
import com.intellij.execution.ExecutionException;
import com.intellij.execution.ExecutionResult;
import com.intellij.execution.Executor;
import com.intellij.execution.configurations.*;
import com.intellij.execution.runners.ExecutionEnvironment;
import com.intellij.execution.runners.ExecutionEnvironmentBuilder;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Key;
import com.microsoft.azure.hdinsight.common.HDInsightUtil;
import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azure.hdinsight.spark.common.SparkBatchRemoteDebugJob;
import com.microsoft.azure.hdinsight.spark.common.SparkBatchRemoteDebugJobSshAuth;
import com.microsoft.azure.hdinsight.spark.common.SparkBatchRemoteDebugJobSshAuth.SSHAuthType;
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitModel;
import com.microsoft.azure.hdinsight.spark.run.configuration.RemoteDebugRunConfiguration;
import com.microsoft.azure.hdinsight.spark.ui.SparkJobLogConsoleView;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.intellij.rxjava.IdeaSchedulers;
import org.apache.commons.lang3.StringUtils;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.net.URI;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class SparkBatchJobDebuggerRunner extends GenericDebuggerRunner {
    public static final Key<String> DebugTargetKey = new Key<>("debug-target");
    private static final Key<String> ProfileNameKey = new Key<>("profile-name");
    public static final String DebugDriver = "driver";
    public static final String DebugExecutor = "executor";

    private boolean isAppInsightEnabled = true;

    @NotNull
    private final List<SparkBatchJobDebugProcessHandler> debugProcessHandlers = new ArrayList<>();

    @Override
    public boolean canRun(@NotNull String executorId, @NotNull RunProfile profile) {
        if (!(profile instanceof RemoteDebugRunConfiguration)) {
            return false;
        }

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
        final SparkBatchJobSubmissionState submissionState = (SparkBatchJobSubmissionState) state;
        final SparkSubmitModel submitModel = submissionState.getSubmitModel();
        final Project project = submitModel.getProject();
        final IdeaSchedulers schedulers = new IdeaSchedulers(project);
        final PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject = PublishSubject.create();
        final PublishSubject<SparkBatchJobSubmissionEvent> debugEventSubject = PublishSubject.create();
        final SparkBatchJobRemoteDebugProcess driverDebugProcess = new SparkBatchJobRemoteDebugProcess(
                schedulers,
                submitModel.getSubmissionParameter(),
                submitModel.getArtifactPath().orElseThrow(() -> new ExecutionException("No artifact selected")),
                submitModel.getAdvancedConfigModel(),
                ctrlSubject);
        final SparkBatchJobDebugProcessHandler driverDebugHandler =
                new SparkBatchJobDebugProcessHandler(project, driverDebugProcess, debugEventSubject);
        driverDebugHandler.getRemoteDebugProcess().start();

        Subscription jobSubscription = ctrlSubject.subscribe(typedMessage -> {
                    switch (typedMessage.getKey()) {
                        case Error:
                            HDInsightUtil.showErrorMessageOnSubmissionMessageWindow(project, typedMessage.getValue());
                            break;
                        case Info:
                            HDInsightUtil.showInfoOnSubmissionMessageWindow(project, typedMessage.getValue());
                            break;
                        case Log:
                            HDInsightUtil.showInfoOnSubmissionMessageWindow(project, typedMessage.getValue());
                            break;
                        case Warning:
                            HDInsightUtil.showWarningMessageOnSubmissionMessageWindow(project, typedMessage.getValue());
                            break;
                    }
                },
                err -> HDInsightUtil.showErrorMessageOnSubmissionMessageWindow(project, err.getMessage()));

        debugEventSubject
                .subscribeOn(Schedulers.io())
                .subscribe(debugEvent -> {
                    try {
                        if (debugEvent instanceof SparkBatchRemoteDebugHandlerReadyEvent) {
                            SparkBatchRemoteDebugHandlerReadyEvent handlerReadyEvent =
                                    (SparkBatchRemoteDebugHandlerReadyEvent) debugEvent;
                            SparkBatchDebugJobJdbPortForwardedEvent jdbReadyEvent =
                                    handlerReadyEvent.getJdbPortForwardedEvent();

                            if (!jdbReadyEvent.getLocalJdbForwardedPort().isPresent()) {
                                return;
                            }

                            int localPort = jdbReadyEvent.getLocalJdbForwardedPort().get();

                            ExecutionEnvironment childEnv = buildChildEnvironment(
                                    environment,
                                    jdbReadyEvent.getRemoteHost().orElse("unknown"),
                                    jdbReadyEvent.isDriver());

                            SparkBatchJobSubmissionState childState = jdbReadyEvent.isDriver() ?
                                    submissionState :
                                    (SparkBatchJobSubmissionState) childEnv.getState();

                            if (childState == null) {
                                return;
                            }

                            if (jdbReadyEvent.isDriver()) {
                                // Let the debug console view to handle the log
                                jobSubscription.unsubscribe();
                            } else {
                                debugProcessHandlers.remove(handlerReadyEvent.getDebugProcessHandler());
                            }

                            // Set the debug connection to localhost and local forwarded port to the state
                            childState.setRemoteConnection(
                                    new RemoteConnection(true, "localhost", Integer.toString(localPort), false));

                            // Prepare the debug tab console view UI
                            SparkJobLogConsoleView jobOutputView = new SparkJobLogConsoleView(project);
                            jobOutputView.attachToProcess(handlerReadyEvent.getDebugProcessHandler());

                            ExecutionResult result = new DefaultExecutionResult(
                                    jobOutputView, handlerReadyEvent.getDebugProcessHandler());
                            childState.setExecutionResult(result);
                            childState.setConsoleView(jobOutputView.getSecondaryConsoleView());
                            childState.setRemoteProcessCtrlLogHandler(handlerReadyEvent.getDebugProcessHandler());

                            // Call supper class method to attach Java virtual machine
                            super.execute(childEnv, jdbReadyEvent.isDriver() ? callback : d -> {}, childState);
                        } else if (debugEvent instanceof SparkBatchJobExecutorCreatedEvent) {
                            SparkBatchJobExecutorCreatedEvent executorCreatedEvent =
                                    (SparkBatchJobExecutorCreatedEvent) debugEvent;

                            final String host = executorCreatedEvent.getHost();
                            final String containerId = executorCreatedEvent.getContainerId();
                            final SparkBatchRemoteDebugJob debugJob =
                                    (SparkBatchRemoteDebugJob) executorCreatedEvent.getJob();

                            URI executorLogUrl = debugJob.getConnectUri().resolve(String.format(
                                    "/yarnui/%s/node/containerlogs/%s/livy",
                                    host,
                                    containerId));

                            // Create an Executor Debug Process
                            SparkBatchJobRemoteDebugExecutorProcess executorDebugProcess =
                                    new SparkBatchJobRemoteDebugExecutorProcess(
                                            schedulers,
                                            submitModel.getSubmissionParameter(),
                                            debugJob,
                                            host,
                                            executorCreatedEvent.getDebugSshSession(),
                                            executorLogUrl.toString());

                            SparkBatchJobDebugProcessHandler executorDebugHandler =
                                    new SparkBatchJobDebugProcessHandler(project, executorDebugProcess, debugEventSubject);

                            executorDebugHandler.getRemoteDebugProcess().start();

                            debugProcessHandlers.add(executorDebugHandler);
                        }
                    } catch (ExecutionException e) {
                        throw new UncheckedExecutionException(e);
                    }
                });
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

}
