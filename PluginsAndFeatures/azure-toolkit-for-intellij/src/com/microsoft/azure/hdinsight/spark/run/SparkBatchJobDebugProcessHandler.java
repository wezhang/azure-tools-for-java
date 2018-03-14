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

package com.microsoft.azure.hdinsight.spark.run;

import com.intellij.debugger.engine.RemoteDebugProcessHandler;
import com.intellij.execution.process.ProcessAdapter;
import com.intellij.execution.process.ProcessEvent;
import com.intellij.execution.process.ProcessOutputTypes;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Key;
import com.intellij.util.concurrency.AppExecutorUtil;
import com.intellij.util.io.BaseOutputReader;
import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.intellij.rxjava.IdeaSchedulers;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.concurrent.Future;

public class SparkBatchJobDebugProcessHandler extends RemoteDebugProcessHandler {
    @NotNull
    private SparkBatchJobRemoteProcess remoteDebugProcess;
//    private final List<SparkBatchJobRemoteDebugExecutorProcess> remoteDebugExecutorProcess = new ArrayList<>();

//    @NotNull
//    private PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject = PublishSubject.create();

//    public SparkBatchJobDebugProcessHandler(Project project) {
//        super(project);
//    }

    @NotNull
    private final PublishSubject<SparkBatchJobSubmissionEvent> debugEventSubject;

    public SparkBatchJobDebugProcessHandler(Project project,
                                            @NotNull SparkBatchJobRemoteProcess remoteDebugProcess,
                                            @NotNull PublishSubject<SparkBatchJobSubmissionEvent> debugEventSubject) {
        super(project);

        this.remoteDebugProcess = remoteDebugProcess;
        this.debugEventSubject = debugEventSubject;
//        this.remoteDebugProcess.start();

        this.remoteDebugProcess.getEventSubject()
//                .observeOn(new IdeaSchedulers(project).processBarVisibleAsync("Listening for remote debug process events"))
                .subscribe(processEvent -> {
                    if (processEvent instanceof SparkBatchDebugJobJdbPortForwardedEvent) {
                        debugEventSubject.onNext(new SparkBatchRemoteDebugHandlerReadyEvent(
                                this, (SparkBatchDebugJobJdbPortForwardedEvent) processEvent));
                    } else {
                        debugEventSubject.onNext(processEvent);
                    }
                });
    }

//    public SparkBatchJobDebugProcessHandler(Project project, SparkSubmitModel submitModel)
//            throws ExecutionException {
//        this(project);
//
//        remoteDebugProcess = new SparkBatchJobRemoteDebugProcess(project, submitModel, ctrlSubject, debugEventSubject);
//
//        ctrlSubject.subscribe(typedMessage -> {
//                    switch (typedMessage.getKey()) {
//                    case Error:
//                        HDInsightUtil.showErrorMessageOnSubmissionMessageWindow(project, typedMessage.getValue());
//                        break;
//                    case Info:
//                        HDInsightUtil.showInfoOnSubmissionMessageWindow(project, typedMessage.getValue());
//                        break;
//                    case Log:
//                        HDInsightUtil.showInfoOnSubmissionMessageWindow(project, typedMessage.getValue());
//                        break;
//                    case Warning:
//                        HDInsightUtil.showWarningMessageOnSubmissionMessageWindow(project, typedMessage.getValue());
//                        break;
//                    }
//                },
//                err -> HDInsightUtil.showErrorMessageOnSubmissionMessageWindow(project, err.getMessage()));
//
////        ColoredRemoteProcessHandler<SparkBatchJobRemoteProcess> jobProcessHandler =
////                new ColoredRemoteProcessHandler<>(remoteDebugProcess, "Starting Spark Job Debug process", null);
//
//        remoteDebugProcess.start();
//
//    }

    @NotNull
    public PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> getCtrlSubject() {
        return getRemoteDebugProcess().getCtrlSubject();
    }

    @NotNull
    public SparkBatchJobRemoteProcess getRemoteDebugProcess() {
        return remoteDebugProcess;
    }

    // A simple log reader to connect the input stream and process handler
    public class SparkBatchJobLogReader extends BaseOutputReader {
        private final Key logType;

        SparkBatchJobLogReader(@NotNull InputStream inputStream, Key logType) {
            super(inputStream, Charset.forName("UTF-8"));
            this.logType = logType;

            start("Reading Spark job log " + logType.toString());
        }

        @Override
        protected void onTextAvailable(@NotNull String s) {
            // Call process handler's text notify
            notifyTextAvailable(s, logType);
        }

        @NotNull
        @Override
        protected Future<?> executeOnPooledThread(@NotNull Runnable runnable) {
            return AppExecutorUtil.getAppExecutorService().submit(runnable);
        }
    }

    @Override
    public void startNotify() {
        addProcessListener(new ProcessAdapter() {
            @Override
            public void startNotified(@NotNull final ProcessEvent event) {
//                if (getRemoteDebugProcess() == null) {
//                    return;
//                }

                final SparkBatchJobLogReader stdoutReader =
                        new SparkBatchJobLogReader(getRemoteDebugProcess().getInputStream(), ProcessOutputTypes.STDOUT);
                final SparkBatchJobLogReader stderrReader =
                        new SparkBatchJobLogReader(getRemoteDebugProcess().getErrorStream(), ProcessOutputTypes.STDERR);

                getRemoteDebugProcess().getCtrlSubject().subscribe(
                        log -> {},
                        err -> {},
                        () -> {
                            stderrReader.stop();
                            stdoutReader.stop();

                            try {
                                stderrReader.waitFor();
                                stdoutReader.waitFor();
                            }
                            catch (InterruptedException ignore) { }
                            finally {
                                removeProcessListener(this);
                            }
                        }
                );
            }
        });

        super.startNotify();
    }

    @Override
    public boolean detachIsDefault() {
        return false;
    }


}
