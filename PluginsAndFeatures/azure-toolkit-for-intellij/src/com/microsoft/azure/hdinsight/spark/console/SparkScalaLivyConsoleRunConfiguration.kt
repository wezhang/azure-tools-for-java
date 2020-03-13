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

package com.microsoft.azure.hdinsight.spark.console

import com.intellij.execution.ExecutionException
import com.intellij.execution.Executor
import com.intellij.execution.configuration.AbstractRunConfiguration
import com.intellij.execution.configurations.*
import com.intellij.execution.runners.ExecutionEnvironment
import com.intellij.execution.runners.ProgramRunner
import com.intellij.openapi.module.Module
import com.intellij.openapi.module.ModuleManager
import com.intellij.openapi.options.SettingsEditor
import com.intellij.openapi.project.Project
import com.intellij.packaging.impl.artifacts.ArtifactUtil
import com.intellij.packaging.impl.run.BuildArtifactsBeforeRunTaskProvider.setBuildArtifactBeforeRun
import com.microsoft.azure.arcadia.sdk.common.livy.interactive.MfaEspSparkSession
import com.microsoft.azure.hdinsight.common.ClusterManagerEx
import com.microsoft.azure.hdinsight.common.MessageInfoType
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail
import com.microsoft.azure.hdinsight.sdk.cluster.LivyCluster
import com.microsoft.azure.hdinsight.sdk.cluster.MfaEspCluster
import com.microsoft.azure.hdinsight.sdk.common.livy.interactive.SparkSession
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitModel
import com.microsoft.azure.hdinsight.spark.run.SparkBatchJobDeployFactory
import com.microsoft.azure.hdinsight.spark.run.configuration.LivySparkBatchJobRunConfiguration
import rx.Observer
import rx.subjects.PublishSubject
import java.net.URI
import java.util.AbstractMap.SimpleImmutableEntry

open class SparkScalaLivyConsoleRunConfiguration(project: Project,
                                                 configurationFactory: SparkScalaLivyConsoleRunConfigurationFactory,
                                                 private val batchRunConfiguration: LivySparkBatchJobRunConfiguration?,
                                                 name: String)
    : AbstractRunConfiguration (
        name, batchRunConfiguration?.configurationModule ?: RunConfigurationModule(project), configurationFactory)
{
    open val runConfigurationTypeName = "HDInsight Spark Run Configuration"

    protected open val submitModel : SparkSubmitModel?
        get() = batchRunConfiguration?.submitModel

    protected open val clusterName : String
        get() = submitModel?.submissionParameter?.clusterName
            ?: throw RuntimeConfigurationWarning("A $runConfigurationTypeName should be selected to start a console")

    protected var cluster: IClusterDetail? = null

    private val consoleBuilder: SparkScalaConsoleBuilder
        get() {
            batchRunConfiguration?.configurationModule?.setModuleToAnyFirstIfNotSpecified()

            return SparkScalaConsoleBuilder(project, batchRunConfiguration?.modules?.firstOrNull()
                    ?: throw ExecutionException(RuntimeConfigurationError(
                            "The default module needs to be set in the local run tab of Run Configuration")))
        }


    override fun getConfigurationEditor(): SettingsEditor<out RunConfiguration> =
            SparkScalaLivyConsoleRunConfigurationEditor()

    override fun getValidModules(): MutableCollection<Module> {
        val moduleName = batchRunConfiguration?.model?.localRunConfigurableModel?.classpathModule
        val moduleManager = ModuleManager.getInstance(project)
        val module = moduleName?.let { moduleManager.findModuleByName(it) }
                ?: moduleManager.modules.first { it.name.equals(project.name, ignoreCase = true) }

        return module?.let { mutableListOf(it) } ?: mutableListOf()
    }

    override fun getState(executor: Executor, env: ExecutionEnvironment): RunProfileState? {
        val logSubject = PublishSubject.create<SimpleImmutableEntry<MessageInfoType, String>>()
        val sparkCluster = cluster ?: throw ExecutionException(RuntimeConfigurationError(
                        "Can't prepare Spark Livy interactive session since the target cluster isn't set or found"))

        val session = createSession(sparkCluster, logSubject)
        applyRunConfiguration(sparkCluster, session)

        return SparkScalaLivyConsoleRunProfileState(consoleBuilder, session, logSubject)
    }

    open fun createSession(sparkCluster: IClusterDetail,
                           logObserver: Observer<SimpleImmutableEntry<MessageInfoType, String>>): SparkSession {
        val url = URI.create((sparkCluster as? LivyCluster)?.livyConnectionUrl
                ?: throw ExecutionException(RuntimeConfigurationError(
                        "Can't prepare Spark interactive session since Livy URL is empty")))

        return if (sparkCluster is MfaEspCluster) MfaEspSparkSession(
                name,
                url,
                sparkCluster.tenantId,
                logObserver)
        else SparkSession(
                name,
                url,
                sparkCluster.httpUserName,
                sparkCluster.httpPassword,
                logObserver)
    }

    open fun applyRunConfiguration(sparkCluster: IClusterDetail, session: SparkSession) {
        val batchSubmitModel = this.batchRunConfiguration?.submitModel ?: throw ExecutionException(
                RuntimeConfigurationError("Can't find Spark batch job configuration to inherit"))

        session.jars = batchSubmitModel.referenceJars
        session.files = batchSubmitModel.referenceFiles
        session.conf = batchSubmitModel.jobConfigs.map { it[0] to it[1] } .toMap()

        val deployDelegate = SparkBatchJobDeployFactory.getInstance().buildSparkBatchJobDeploy(
                batchSubmitModel, sparkCluster)

        batchSubmitModel.artifactPath.ifPresent {
            session.deploy = deployDelegate
            synchronized(session.artifactsToDeploy) {
                session.artifactsToDeploy.clear()
                session.artifactsToDeploy.add(it)
            }
        }

        if (!batchSubmitModel.isLocalArtifact) {
            ArtifactUtil.getArtifactWithOutputPaths(project)
                    .first { artifact -> artifact.name == batchSubmitModel.artifactName }
                    ?.let { setBuildArtifactBeforeRun(project, this, it) }
        }
    }

    override fun checkRunnerSettings(runner: ProgramRunner<*>, runnerSettings: RunnerSettings?, configurationPerRunnerSettings: ConfigurationPerRunnerSettings?) {
        cluster = ClusterManagerEx.getInstance().getClusterDetailByName(clusterName)
                .orElseThrow { RuntimeConfigurationError("Can't find the target cluster $clusterName") }
    }
}
