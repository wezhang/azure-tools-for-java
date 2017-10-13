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

import com.intellij.debugger.DebugUIEnvironment;
import com.intellij.debugger.engine.RemoteDebugProcessHandler;
import com.intellij.execution.*;
import com.intellij.execution.configurations.*;
import com.intellij.execution.executors.DefaultDebugExecutor;
import com.intellij.execution.executors.DefaultRunExecutor;
import com.intellij.execution.impl.ConsoleViewImpl;
import com.intellij.execution.process.KillableColoredProcessHandler;
import com.intellij.execution.process.OSProcessHandler;
import com.intellij.execution.process.ProcessTerminatedListener;
import com.intellij.execution.runners.ProgramRunner;
import com.intellij.execution.util.JavaParametersUtil;
import com.intellij.openapi.project.Project;
import com.intellij.util.PathUtil;
import com.intellij.xdebugger.impl.ui.DebuggerUIUtil;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.spark.common.*;
import com.microsoft.azure.hdinsight.spark.mock.SparkLocalRunner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;

/**
 * Spark Batch Job Submission Run profile state
 */
public class SparkBatchJobSubmissionState implements RunProfileState, RemoteState {
    private final Project myProject;
    private RemoteConnection remoteConnection;
    @NotNull
    private SparkBatchJobConfigurableModel jobModel;

    public SparkBatchJobSubmissionState(@NotNull Project project, @NotNull SparkBatchJobConfigurableModel jobModel) {
        this.myProject = project;
        this.jobModel = jobModel;
    }

    public void setRemoteConnection(RemoteConnection remoteConnection) {
        this.remoteConnection = remoteConnection;
    }

    public SparkSubmitModel getSubmitModel() {
        return jobModel.getSubmitModel();
    }

    @Nullable
    @Override
    public ExecutionResult execute(Executor executor, @NotNull ProgramRunner programRunner) throws ExecutionException {
        if (executor instanceof SparkBatchJobDebugExecutor) {
            ConsoleViewImpl consoleView = new ConsoleViewImpl(myProject, false);
            SparkBatchJobDebugProcessHandler process = new SparkBatchJobDebugProcessHandler(myProject);

            consoleView.attachToProcess(process);

            ExecutionResult result = new DefaultExecutionResult(consoleView, process);
            programRunner.onProcessStarted(null, result);

            return result;
//      } else if (programRunner instanceof SparkBatchJobRunner) {
//          SparkBatchJobRunner jobRunner = (SparkBatchJobRunner) programRunner;
//          jobRunner.submitJob(getSubmitModel());
        } else if (executor instanceof DefaultRunExecutor || executor instanceof DefaultDebugExecutor) {
            // Spark Local Run
            ConsoleViewImpl consoleView = new ConsoleViewImpl(myProject, false);
            OSProcessHandler processHandler = new KillableColoredProcessHandler(
                    createCommandlineForLocal(jobModel.getLocalRunConfigurableModel(), executor instanceof DefaultDebugExecutor));

            consoleView.attachToProcess(processHandler);

            return new DefaultExecutionResult(consoleView, processHandler);
        }

        return null;
    }

    private GeneralCommandLine createCommandlineForLocal(SparkLocalRunConfigurableModel localRunConfigurableModel, Boolean isDebug) throws ExecutionException {
        JavaParameters params = new JavaParameters();
        JavaParametersUtil.configureConfiguration(params, localRunConfigurableModel);
        JavaParametersUtil.configureProject(myProject, params, JavaParameters.JDK_AND_CLASSES_AND_TESTS, null);

        params.setWorkingDirectory(
                Paths.get(localRunConfigurableModel.getDataRootDirectory(), "__default__", "user", "current").toString());

        // Add jmockit as -javaagent
        String jmockitJarPath = params.getClassPath().getPathList().stream()
                .filter(path -> path.toLowerCase().matches(".*\\Wjmockit-.*\\.jar"))
                .findFirst()
                .orElseThrow(() -> new ExecutionException("Dependence jmockit not found"));
        String javaAgentParam = "-javaagent:" + jmockitJarPath;
        params.getVMParametersList().add(javaAgentParam);

        if (isDebug) {
            // TODO: Add onthrow and onuncaught with Breakpoint UI settings later
            String debugConnection = String.format("-agentlib:jdwp=transport=dt_socket,server=n,address=127.0.0.1:%s,suspend=y", getRemoteConnection().getAddress());

            params.getVMParametersList().add(debugConnection);
        }

        params.getClassPath().add(PathUtil.getJarPathForClass(SparkLocalRunner.class));

        params.getProgramParametersList()
                .addAt(0,
                        Optional.ofNullable(localRunConfigurableModel.getRunClass())
                                .filter(mainClass -> !mainClass.trim().isEmpty())
                                .orElseThrow(() -> new ExecutionException("Spark job's main class isn't set")));

        params.setMainClass(SparkLocalRunner.class.getCanonicalName());
        return params.toCommandLine();
    }

    @Override
    public RemoteConnection getRemoteConnection() {
        if (this.remoteConnection == null) {
            setRemoteConnection(new RemoteConnection(true, "127.0.0.1", "0", true));
        }

        return this.remoteConnection;
    }
}
