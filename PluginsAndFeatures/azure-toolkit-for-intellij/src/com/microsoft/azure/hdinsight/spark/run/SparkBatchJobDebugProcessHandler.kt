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
 */

package com.microsoft.azure.hdinsight.spark.run

import com.intellij.debugger.engine.RemoteDebugProcessHandler
import com.intellij.execution.process.ProcessAdapter
import com.intellij.execution.process.ProcessEvent
import com.intellij.execution.process.ProcessOutputTypes
import com.intellij.openapi.project.Project
import com.intellij.openapi.util.Key
import com.intellij.util.concurrency.AppExecutorUtil
import com.intellij.util.io.BaseOutputReader
import com.microsoft.azure.hdinsight.common.MessageInfoType
import com.microsoft.intellij.rxjava.IdeaSchedulers
import rx.schedulers.Schedulers
import rx.subjects.PublishSubject

import java.io.InputStream
import java.nio.charset.Charset
import java.util.AbstractMap.SimpleImmutableEntry
import java.util.concurrent.Future

class SparkBatchJobDebugProcessHandler(project: Project,
                                       val remoteDebugProcess: SparkBatchJobRemoteProcess,
                                       debugEventSubject: PublishSubject<SparkBatchJobSubmissionEvent>)
    : RemoteDebugProcessHandler(project) {


    val ctrlSubject: PublishSubject<SimpleImmutableEntry<MessageInfoType, String>>
        get() = remoteDebugProcess.ctrlSubject

    init {
        //        this.remoteDebugProcess.start();

        this.remoteDebugProcess.eventSubject
                //                .observeOn(new IdeaSchedulers(project).processBarVisibleAsync("Listening for remote debug process events"))
                .subscribe { processEvent ->
                    if (processEvent is SparkBatchDebugJobJdbPortForwardedEvent) {
                        debugEventSubject.onNext(SparkBatchRemoteDebugHandlerReadyEvent(this, processEvent))
                    } else {
                        debugEventSubject.onNext(processEvent)
                    }
                }
    }

    // A simple log reader to connect the input stream and process handler
    inner class SparkBatchJobLogReader internal constructor(inputStream: InputStream, private val logType: Key<*>)
        : BaseOutputReader(inputStream, Charset.forName("UTF-8")) {

        init {
            start("Reading Spark job log " + logType.toString())
        }

        override fun onTextAvailable(s: String) {
            // Call process handler's text notify
            notifyTextAvailable(s, logType)
        }

        override fun executeOnPooledThread(runnable: Runnable): Future<*> {
            return AppExecutorUtil.getAppExecutorService().submit(runnable)
        }
    }

    override fun startNotify() {
        addProcessListener(object : ProcessAdapter() {
            override fun startNotified(event: ProcessEvent) {
                val stdoutReader = SparkBatchJobLogReader(remoteDebugProcess.inputStream, ProcessOutputTypes.STDOUT)
                val stderrReader = SparkBatchJobLogReader(remoteDebugProcess.errorStream, ProcessOutputTypes.STDERR)

                remoteDebugProcess.ctrlSubject.subscribe(
                        { },
                        { },
                        {
                            stderrReader.stop()
                            stdoutReader.stop()

                            try {
                                stderrReader.waitFor()
                                stdoutReader.waitFor()
                            } catch (ignore: InterruptedException) {
                            } finally {
                                removeProcessListener(this)
                            }
                        })
            }
        })

        super.startNotify()
    }

    override fun detachIsDefault(): Boolean = false
}
