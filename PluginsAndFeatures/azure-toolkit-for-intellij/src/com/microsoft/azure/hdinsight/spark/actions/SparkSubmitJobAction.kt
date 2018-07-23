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

package com.microsoft.azure.hdinsight.spark.actions

import com.intellij.execution.ExecutorRegistry
import com.intellij.execution.ProgramRunnerUtil
import com.intellij.openapi.actionSystem.AnActionEvent
import com.intellij.openapi.actionSystem.DataContext
import com.microsoft.azure.hdinsight.spark.actions.SparkDataKeys.*
import com.microsoft.azure.hdinsight.spark.run.SparkBatchJobRunExecutor
import com.microsoft.azure.hdinsight.spark.run.configuration.*
import com.microsoft.azuretools.ijidea.utility.AzureAnAction
import javax.swing.Icon

class SparkSubmitJobAction(text: String?, description: String?, icon: Icon?) : AzureAnAction(text, description, icon) {
    override fun onActionPerformed(anActionEvent: AnActionEvent?) {
        if (anActionEvent == null) {
            return
        }

        submit(anActionEvent.dataContext)
    }

    fun submit(context: DataContext) {
        val runConfigurationSetting = context.getData(RUN_CONFIGURATION_SETTING) ?: return
        val clusterName = context.getData(CLUSTER)?.name
        val mainClassName = context.getData(MAIN_CLASS_NAME)

        val executor = ExecutorRegistry.getInstance().getExecutorById(SparkBatchJobRunExecutor.EXECUTOR_ID)

        runConfigurationSetting.isEditBeforeRun = true

        val runConfiguration = runConfigurationSetting.configuration as RemoteDebugRunConfiguration
        val model = runConfiguration.model
        model.focusedTabIndex = 1   // Select remote job submission tab

        if (clusterName != null) {
            model.submitModel.submissionParameter.clusterName = clusterName     // Select the cluster
            model.isClusterSelectionEnabled = false
        }

        if (mainClassName != null) {
            model.submitModel.submissionParameter.clusterName = mainClassName
        }

        model.isLocalRunConfigEnabled = false   // Disable local run configuration tab

        ProgramRunnerUtil.executeConfiguration(runConfigurationSetting, executor)

        // Restore for common run configuration editor
        runConfigurationSetting.isEditBeforeRun = false
        model.isLocalRunConfigEnabled = true
        if (clusterName != null) {
            model.isClusterSelectionEnabled = true
        }
    }
}