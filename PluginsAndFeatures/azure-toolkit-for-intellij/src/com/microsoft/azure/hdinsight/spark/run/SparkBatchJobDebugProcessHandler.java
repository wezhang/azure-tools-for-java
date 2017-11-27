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
import com.intellij.execution.ExecutionException;
import com.intellij.execution.process.ProcessAdapter;
import com.intellij.execution.process.ProcessEvent;
import com.intellij.execution.process.ProcessOutputTypes;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Key;
import com.intellij.remote.BaseRemoteProcessHandler;
import com.intellij.remote.ColoredRemoteProcessHandler;
import com.intellij.util.concurrency.AppExecutorUtil;
import com.intellij.util.io.BaseOutputReader;
import com.microsoft.azure.hdinsight.common.MessageInfoType;
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitModel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import rx.Observer;
import rx.subjects.PublishSubject;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.concurrent.Future;

public class SparkBatchJobDebugProcessHandler extends RemoteDebugProcessHandler {
    private final SparkBatchJobRemoteDebugProcess remoteDebugProcess;
    @NotNull PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject = PublishSubject.create();

    public SparkBatchJobDebugProcessHandler(Project project, SparkSubmitModel submitModel)
            throws ExecutionException {
        super(project);

        remoteDebugProcess = new SparkBatchJobRemoteDebugProcess(project, submitModel, ctrlSubject);

//        ColoredRemoteProcessHandler<SparkBatchJobRemoteProcess> jobProcessHandler =
//                new ColoredRemoteProcessHandler<>(remoteDebugProcess, "Starting Spark Job Debug process", null);

        remoteDebugProcess.start();
    }

    public PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> getCtrlSubject() {
        return ctrlSubject;
    }

    public SparkBatchJobRemoteProcess getDebugProcess() {
        return remoteDebugProcess;
    }

    class SparkBatchJobLogReader extends BaseOutputReader {
        private final Key logType;

        SparkBatchJobLogReader(@NotNull InputStream inputStream, Key logType) {
            super(inputStream, Charset.forName("UTF-8"));
            this.logType = logType;
        }

        @Override
        protected void onTextAvailable(@NotNull String s) {
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
            public void startNotified(final ProcessEvent event) {
                try {
                    final SparkBatchJobLogReader stdoutReader =
                            new SparkBatchJobLogReader(remoteDebugProcess.getInputStream(), ProcessOutputTypes.STDOUT);
                    final SparkBatchJobLogReader stderrReader =
                            new SparkBatchJobLogReader(remoteDebugProcess.getErrorStream(), ProcessOutputTypes.STDERR);

                    ctrlSubject.subscribe(
                            log -> {},
                            err -> {},
                            () -> {
                                try {
                                    stderrReader.waitFor();
                                    stdoutReader.waitFor();
                                }
                                catch (InterruptedException ignore) { }
                            }
                    );
                }
                finally {
                    removeProcessListener(this);
                }
            }
        });

        super.startNotify();
    }

    @Override
    public boolean detachIsDefault() {
        return false;
    }


}
