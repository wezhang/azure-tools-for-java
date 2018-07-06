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

package com.microsoft.azure.hdinsight.spark.run

import com.intellij.execution.ExecutorRegistry
import com.intellij.execution.ProgramRunnerUtil
import com.intellij.execution.RunManager
import com.intellij.openapi.project.Project
import com.microsoft.azure.hdinsight.spark.run.configuration.ServerlessSparkConfiguration
import com.microsoft.azure.hdinsight.spark.run.configuration.ServerlessSparkConfigurationFactory
import com.microsoft.azure.hdinsight.spark.run.configuration.ServerlessSparkConfigurationType

class ServerlessSparkRunConfigurationSubmitter(project: Project, val clusterName: String?) {
    var runConfigurationSetting = RunManager.getInstance(project).createRunConfiguration(
            "[Spark Job] To ADL pool $clusterName",
            ServerlessSparkConfigurationFactory(ServerlessSparkConfigurationType()))
    var runConfiguration = runConfigurationSetting.configuration as ServerlessSparkConfiguration

    var executor = ExecutorRegistry.getInstance().getExecutorById(SparkBatchJobRunExecutor.EXECUTOR_ID)

    fun submit() {
        runConfigurationSetting.isEditBeforeRun = true

        val model = runConfiguration.model
        model.focusedTabIndex = 1   // Select remote job submission tab
        model.submitModel.submissionParameter.clusterName = clusterName     // Select the cluster

        ProgramRunnerUtil.executeConfiguration(runConfigurationSetting, executor)
    }
}