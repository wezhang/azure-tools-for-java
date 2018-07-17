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

import com.intellij.execution.ExecutionException
import com.intellij.execution.configurations.RunProfile
import com.microsoft.azure.hdinsight.common.MessageInfoType
import com.microsoft.azure.hdinsight.sdk.common.azure.serverless.AzureSparkServerlessClusterManager
import com.microsoft.azure.hdinsight.spark.common.*
import com.microsoft.azure.hdinsight.spark.run.configuration.ServerlessSparkConfiguration
import rx.Observer
import java.util.AbstractMap.SimpleImmutableEntry

class ServerlessSparkBatchRunner : SparkBatchJobRunner() {
    override fun canRun(executorId: String, profile: RunProfile): Boolean {
        return SparkBatchJobRunExecutor.EXECUTOR_ID == executorId && profile is ServerlessSparkConfiguration
    }

    override fun getRunnerId(): String {
        return "ServerlessSparkBatchRun"
    }

    @Throws(ExecutionException::class)
    override fun buildSparkBatchJob(submitModel: SparkSubmitModel, ctrlSubject: Observer<SimpleImmutableEntry<MessageInfoType, String>>): ISparkBatchJob {
        val tenantId = (submitModel as ServerlessSparkSubmitModel).tenantId
        val accountName = submitModel.accountName

        if (submitModel.clusterId == null) {
            throw ExecutionException("Can't get the Azure Serverless Spark cluster, please sign in and refresh.")
        }

        val clusterId = submitModel.clusterId
        try {
            val livyUri = submitModel.livyUri ?: AzureSparkServerlessClusterManager.getInstance()
                    .findCluster(accountName, clusterId)
                    .map { it.get().toBlocking().singleOrDefault(it).livyUri }
                    .toBlocking()
                    .firstOrDefault(null)

            return ServerlessSparkBatchJob(
                    submitModel.submissionParameter,
                    SparkBatchAzureSubmission(tenantId, accountName, clusterId, livyUri),
                    ctrlSubject)
        } catch (e: Exception) {
            throw ExecutionException("Can't get the Azure Serverless Spark cluster, please sign in and refresh.", e)
        }

    }
}