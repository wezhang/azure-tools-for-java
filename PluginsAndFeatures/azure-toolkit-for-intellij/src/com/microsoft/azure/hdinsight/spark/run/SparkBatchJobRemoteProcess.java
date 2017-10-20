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
 */

package com.microsoft.azure.hdinsight.spark.run;

import com.google.common.net.HostAndPort;
import com.intellij.execution.ExecutionException;
import com.intellij.openapi.compiler.CompilationException;
import com.intellij.openapi.compiler.CompileScope;
import com.intellij.openapi.compiler.CompilerManager;
import com.intellij.openapi.compiler.CompilerMessageCategory;
import com.intellij.openapi.project.Project;
import com.intellij.packaging.artifacts.Artifact;
import com.intellij.packaging.impl.compiler.ArtifactCompileScope;
import com.intellij.packaging.impl.compiler.ArtifactsWorkspaceSettings;
import com.intellij.remote.RemoteProcess;
import com.microsoft.azure.hdinsight.common.ClusterManagerEx;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.sdk.common.HDIException;
import com.microsoft.azure.hdinsight.sdk.common.NotSupportExecption;
import com.microsoft.azure.hdinsight.spark.common.*;
import hidden.edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import rx.Single;
import rx.SingleSubscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URI;
import java.util.Collections;
import java.util.Set;

public class SparkBatchJobRemoteProcess extends RemoteProcess {
    @NotNull
    private Project project;
    @NotNull
    private SparkSubmitModel submitModel;
    @Nullable
    private SparkJobLogReader jobStdoutLogReader;
    @Nullable
    private InputStream jobStdoutLogInputSteam;

    public SparkBatchJobRemoteProcess(@NotNull Project project, @NotNull SparkSubmitModel sparkSubmitModel) {
        this.project = project;
        this.submitModel = sparkSubmitModel;
    }

    /**
     * To Kill the remote job.
     *
     * @return is the remote Spark Job killed
     */
    @Override
    public boolean killProcessTree() {
        return true;
    }

    /**
     * Is the Spark job session connected
     *
     * @return is the Spark Job log getting session still connected
     */
    @Override
    public boolean isDisconnected() {
        return false;
    }

    @Nullable
    @Override
    public HostAndPort getLocalTunnel(int i) {
        return null;
    }

    @Override
    public OutputStream getOutputStream() {
        return new NullOutputStream();
    }

    @Override
    public InputStream getInputStream() {
        return jobStdoutLogInputSteam;
    }

    @Override
    public InputStream getErrorStream() {
        return new ReaderInputStream(new StringReader("!@#$%"), "utf-8");
    }

    @Override
    public int waitFor() throws InterruptedException {
        return 0;
    }

    @Override
    public int exitValue() {
        return 0;
    }

    @Override
    public void destroy() {

    }

    Single<Artifact> buildArtifact() {
        return Single.create(ob -> {
            if (submitModel.isLocalArtifact()) {
                ob.onError(new NotSupportExecption());
                return;
            }

            final Set<Artifact> artifacts = Collections.singleton(submitModel.getArtifact());
            ArtifactsWorkspaceSettings.getInstance(project).setArtifactsToBuild(artifacts);

            final CompileScope scope = ArtifactCompileScope.createArtifactsScope(project, artifacts, true);
            CompilerManager.getInstance(project).make(scope, (aborted, errors, warnings, compileContext) -> {
                if (aborted || errors != 0) {
                    ob.onError(new CompilationException(Arrays.toString(compileContext.getMessages(CompilerMessageCategory.ERROR))));
                } else {
                    ob.onSuccess(submitModel.getArtifact());
                }
            });
        });
    }

    Single<IClusterDetail> deployArtifact(@NotNull Artifact artifact, @NotNull String clusterName) {
        return Single.create(ob -> {
            try {
                IClusterDetail clusterDetail = ClusterManagerEx.getInstance()
                        .getClusterDetailByName(clusterName)
                        .orElseThrow(() -> new HDIException("No cluster name matched selection: " + clusterName));

                submitModel.uploadFileToCluster(clusterDetail, artifact.getName());

                ob.onSuccess(clusterDetail);
            } catch (Exception e) {
                ob.onError(e);
            }
        });
    }

    Single<SparkBatchJob> submit(@NotNull IClusterDetail cluster, @NotNull SparkSubmissionParameter parameter) {
        return Single.create((SingleSubscriber<? super SparkBatchJob> ob) -> {
            SparkBatchJob sparkJob = new SparkBatchJob(
                    URI.create(SparkSubmitHelper.getLivyConnectionURL(cluster)),
                    parameter,
                    SparkBatchSubmission.getInstance());

            // would block a while
            try {
                sparkJob.createBatchJob();
                ob.onSuccess(sparkJob);
            } catch (IOException e) {
                ob.onError(e);
            }
        });
    }

    public void start() {
        buildArtifact()
                .flatMap(artifact -> deployArtifact(artifact, submitModel.getSubmissionParameter().getClusterName()))
                .flatMap(cluster -> submit(cluster, submitModel.getSubmissionParameter()))
                .subscribe(
                        job -> {
                            this.jobStdoutLogReader = new SparkJobLogReader(job, "stdout");
                            this.jobStdoutLogInputSteam = new ReaderInputStream(jobStdoutLogReader, "utf-8");
                        },
                        err -> {

                        }
                );
    }
}
