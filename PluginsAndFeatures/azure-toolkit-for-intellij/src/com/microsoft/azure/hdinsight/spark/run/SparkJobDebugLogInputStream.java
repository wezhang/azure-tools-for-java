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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;

import java.io.*;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkJobDebugLogInputStream extends FilterInputStream {
    class SparkJobExecutor {
        @NotNull
        final String host;

        @NotNull
        final String containerId;

        public SparkJobExecutor(@NotNull String host, @NotNull String containerId) {
            this.host = host;
            this.containerId = containerId;
        }
    }

    private static final Pattern executorLogUrlPattern = Pattern.compile(
            "^\\s+SPARK_LOG_URL_STDERR -> https?://([^:]+):?\\d*/node/containerlogs/(container.*)/livy/stderr.*");

    @NotNull
    private InputStreamReader inputStreamReader = new InputStreamReader(in);

    @NotNull
    private BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

    @NotNull
    public PublishSubject<SparkJobExecutor> getExecutorSubject() {
        return executorSubject;
    }

    @NotNull
    private PublishSubject<SparkJobExecutor> executorSubject = PublishSubject.create();
    @NotNull
    private Subscription executorLogMatchSubscription;
    @NotNull
    private PublishSubject<String> logSubject = PublishSubject.create();

    @Nullable
    private String line;
    private int pos = 0;

    public SparkJobDebugLogInputStream(@NotNull InputStream in) {
        super(in);

        this.executorLogMatchSubscription = matchExecutorFromDebugProcessObservable(logSubject);
    }

    @Override
    public int read() throws IOException {
        return getLine()
                .map(String::getBytes)
                .filter(bufferBytes -> bufferBytes.length > pos)
                .map(bufferBytes -> (int) bufferBytes[pos++])
                .orElseGet(() -> {
                    try {
                        line = bufferedReader.readLine();
                        pos = 0;

                        if (line.isEmpty()) {
                            return -1;
                        }

                        logSubject.onNext(line);

                        return (int) line.getBytes()[0];
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    Optional<String> getLine() {
        return Optional.ofNullable(line);
    }

    @Override
    public void close() throws IOException {
        super.close();

        this.logSubject.onCompleted();
        this.executorSubject.onCompleted();
        this.executorLogMatchSubscription.unsubscribe();
        bufferedReader.close();
    }

    /**
     * To match Executor lunch content from debug process Observable
     *
     * @param debugProcessOb the debug process Observable to match
     * @return matched Executor Observable, the event is SimpleEntry with host, containerId pair
     */
    private Subscription matchExecutorFromDebugProcessObservable(Observable<String> debugProcessOb) {
        PublishSubject<String> closeSubject = PublishSubject.create();
        PublishSubject<String> openSubject = PublishSubject.create();

        return debugProcessOb
                .map(line -> {
                    if (line.matches("^YARN executor launch context:$")) {
                        openSubject.onNext("YARN executor launch");
                    }

                    if (line.matches("^={5,}$")) {
                        closeSubject.onNext("=====");
                    }

                    return line;
                })
                .window(openSubject, s -> closeSubject)
                .flatMap(executorLunchContextOb -> executorLunchContextOb
                        .map(executorLogUrlPattern::matcher)
                        .filter(Matcher::matches)
                        .map(matcher -> new SparkJobExecutor(matcher.group(1), matcher.group(2)))
                )
                .subscribe(executor -> executorSubject.onNext(executor));
    }
}
