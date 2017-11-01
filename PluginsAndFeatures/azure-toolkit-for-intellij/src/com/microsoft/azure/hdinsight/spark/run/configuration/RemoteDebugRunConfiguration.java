/**
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
package com.microsoft.azure.hdinsight.spark.run.configuration;

import com.intellij.execution.ExecutionException;
import com.intellij.execution.Executor;
import com.intellij.execution.configurations.*;
import com.intellij.execution.runners.ExecutionEnvironment;
import com.intellij.openapi.module.Module;
import com.intellij.openapi.options.SettingsEditor;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.InvalidDataException;
import com.intellij.openapi.util.WriteExternalException;
import com.microsoft.azure.hdinsight.spark.common.SparkBatchJobConfigurableModel;
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitModel;
import com.microsoft.azure.hdinsight.spark.run.SparkBatchJobDebugExecutor;
import com.microsoft.azure.hdinsight.spark.run.SparkBatchJobRunExecutor;
import com.microsoft.azure.hdinsight.spark.run.SparkBatchJobSubmissionState;
import com.microsoft.azuretools.telemetry.AppInsightsClient;
import com.microsoft.intellij.hdinsight.messages.HDInsightBundle;
import org.jdom.Element;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RemoteDebugRunConfiguration extends ModuleBasedConfiguration<RunConfigurationModule>
{
    @NotNull
    private SparkBatchJobConfigurableModel jobModel;

    public RemoteDebugRunConfiguration(@NotNull Project project, @NotNull ConfigurationFactory factory, @NotNull RunConfigurationModule configurationModule, String name) {
        super(name, configurationModule, factory);

        this.jobModel = new SparkBatchJobConfigurableModel(project);
    }

    @Override
    public void readExternal(Element rootElement) throws InvalidDataException {
        super.readExternal(rootElement);

        jobModel.applyFromElement(rootElement);
    }

    @Override
    public void writeExternal(Element rootElement) throws WriteExternalException {
        super.writeExternal(rootElement);

        Element jobConfigElement = jobModel.exportToElement();
        rootElement.addContent(jobConfigElement);
    }

    @NotNull
    public SparkBatchJobConfigurableModel getModel() {
        return jobModel;
    }

    @NotNull
    public SparkSubmitModel getSubmitModel() {
        return getModel().getSubmitModel();
    }

    @NotNull
    @Override
    public SettingsEditor<? extends RunConfiguration> getConfigurationEditor() {
        return new RemoteDebugSettingsEditor(this);
    }

    @Override
    public void checkConfiguration() throws RuntimeConfigurationException {

    }

    @Nullable
    @Override
    public RunProfileState getState(@NotNull Executor executor, @NotNull ExecutionEnvironment executionEnvironment) throws ExecutionException {
        SparkBatchJobSubmissionState state = new SparkBatchJobSubmissionState(getProject(), jobModel);

        createAppInsightEvent(executor, new HashMap<String, String>() {{
            put("Executor", executor.getId());
            put("ActionUuid", state.getUuid());
        }});

        return state;
    }

    @Override
    public Collection<Module> getValidModules() {
        return new ArrayList<>();
    }

    public void setAsNew() {
    }

    public static void createAppInsightEvent(@NotNull Executor executor, @NotNull final Map<String, String> postEventProps) {
        switch (executor.getId()) {
            case "Run":
                AppInsightsClient.create(HDInsightBundle.message("SparkRunConfigLocalRunButtonClick"), null, postEventProps);
                break;
            case "Debug":
                AppInsightsClient.create(HDInsightBundle.message("SparkRunConfigLocalDebugButtonClick"), null, postEventProps);
                break;
            case SparkBatchJobRunExecutor.EXECUTOR_ID:
                AppInsightsClient.create(HDInsightBundle.message("SparkRunConfigRunButtonClick"), null, postEventProps);
                break;
            case SparkBatchJobDebugExecutor.EXECUTOR_ID:
                AppInsightsClient.create(HDInsightBundle.message("SparkRunConfigDebugButtonClick"), null, postEventProps);
                break;
        }
    }
}

