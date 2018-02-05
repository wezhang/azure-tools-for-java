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

package com.microsoft.azure.hdinsight.sdk.common.livy.interactive;

import com.microsoft.azure.hdinsight.sdk.common.livy.interactive.exceptions.StatementNotStartException;
import com.microsoft.azure.hdinsight.sdk.rest.livy.interactive.StatementOutput;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import rx.Observable;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

public class Statement {
    @NotNull
    private URL baseUrl;            // Statement base URL
    @NotNull
    private Session session;        // Statement owner session
    private int id;                 // Statement ID of server
    @Nullable
    private StatementOutput output; // Statement outputs
    @NotNull
    private InputStream codeStream; // Codes to run

    public Statement(@NotNull Session session, @NotNull InputStream codeStream) {
        this.session = session;
    }

    @NotNull
    public InputStream getCodeInputStream() {
        return codeStream;
    }

    @NotNull
    public URI getUri() throws StatementNotStartException {
        throw new NotImplementedException();
    }

    public int getId() throws StatementNotStartException {
        throw new NotImplementedException();
    }

    @NotNull
    public Session getSession() {
        return this.session;
    }

    /*
     * Observable APIs, all IO operations
     */

    public Observable<Statement> run() {
        throw new NotImplementedException();
    }

    public Observable<Statement> cancel() {
        throw new NotImplementedException();
    }
}
