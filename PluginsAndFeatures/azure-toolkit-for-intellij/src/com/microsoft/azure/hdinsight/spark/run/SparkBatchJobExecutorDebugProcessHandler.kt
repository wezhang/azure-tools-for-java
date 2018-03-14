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
import com.intellij.openapi.project.Project
import com.microsoft.azure.hdinsight.spark.common.SparkBatchDebugSession
import com.microsoft.azure.hdinsight.spark.common.SparkBatchRemoteDebugJob
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitModel

class SparkBatchJobExecutorDebugProcessHandler
//@Throws(ExecutionException::class)
//    ://constructor(project: Project,
//            submitModel: SparkSubmitModel,
//            sshDebugSession: SparkBatchDebugSession,
//            debugJob: SparkBatchRemoteDebugJob,
//            host: String,
//            containerId: String
//) : SparkBatchJobDebugProcessHandler(project) {
//    private val remoteExecutorDebugProcess: SparkBatchJobRemoteDebugExecutorProcess
//
//    init {
////        remoteDebugProcess.eventSubject
////                .filter { it is SparkBatchJobExecutorCreatedEvent }
////                .map { it as SparkBatchJobExecutorCreatedEvent }
////                .subscribe { executorCreatedEvent ->
////                    val sshDebugSession = (remoteDebugProcess as SparkBatchJobRemoteDebugProcess).sshDebugSession
//
////                    if (sshDebugSession == null) {
////                        ctrlSubject.onError(ExecutionException ("No SSH session for debugging!"))
////
////                        return
////                    }
//
//                    val executorLogUrl = debugJob.connectUri.resolve(
//                            "/yarnui/$host/node/containerlogs/$containerId/livy")
//
////                    remoteExecutorDebugProcess = SparkBatchJobRemoteDebugExecutorProcess(
////                            project,
////                            submitModel,
////                            ctrlSubject,
////                            debugEventSubject,
////                            debugJob,
////                            host,
////                            sshDebugSession, executorLogUrl.toString())
////
//////                    remoteDebugExecutorProcess.add(executorProcess)
////                    remoteExecutorDebugProcess.start()
////                }
//    }
//
//    override fun getRemoteDebugProcess(): SparkBatchJobRemoteProcess {
//        return remoteExecutorDebugProcess
//    }
//}
