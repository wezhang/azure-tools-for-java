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

import com.microsoft.azure.hdinsight.common.StreamUtil;
import com.microsoft.azure.hdinsight.sdk.common.HDIException;
import com.microsoft.azure.hdinsight.sdk.common.HttpObservable;
import com.microsoft.azure.hdinsight.sdk.common.livy.interactive.exceptions.SessionNotStartException;
import com.microsoft.azure.hdinsight.sdk.rest.ObjectConvertUtils;
import com.microsoft.azure.hdinsight.sdk.rest.livy.interactive.SessionKind;
import com.microsoft.azure.hdinsight.sdk.rest.livy.interactive.SessionState;
import com.microsoft.azure.hdinsight.sdk.rest.livy.interactive.api.PostSessions;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import org.apache.http.entity.StringEntity;
import rx.Observable;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;

import static rx.exceptions.Exceptions.propagate;

public abstract class Session {
    public static final String REST_SEGMENT = "sessions";

    @NotNull
    private URL baseUrl;            // Session base URL

    private int id;                 // Session ID of server

    @Nullable
    private String appId;           // Application ID of server

    @NotNull
    private HttpObservable http;    // Http connection

    @NotNull
    private String name;

    /*
     * Constructor
     */

    public Session(@NotNull String name, @NotNull URL baseUrl) {
        this.name = name;
        this.baseUrl = baseUrl;
        this.http = new HttpObservable();
    }

    public Session(@NotNull String name, @NotNull final URL baseUrl, @NotNull final String username, @NotNull final String password) {
        this.name = name;
        this.baseUrl = baseUrl;
        this.http = new HttpObservable(username, password);
    }

    /*
     * Getter / Setter
     */
    @NotNull
    public String getName() {
        return name;
    }

    public void setName(@NotNull String name) {
        this.name = name;
    }

    @NotNull
    public URI getUri() throws SessionNotStartException {
        throw new NotImplementedException();
    }

    private void setId(int id) {
        this.id = id;
    }

    public int getId() throws SessionNotStartException {
        return id;
    }

    @Nullable
    public String getAppId() {
        return appId;
    }

    private void setAppId(@Nullable String appId) {
        this.appId = appId;
    }

    @NotNull
    public abstract SessionKind getKind();

    @NotNull
    public SessionState getState() {
        throw new NotImplementedException();
    }

    @NotNull
    public HttpObservable getHttp() {
        return http;
    }

    /*
     * Observable APIs, all IO operations
     */

    /**
     * To create a session with specified kind.
     *
     * @return An updated Session instance Observable
     */
    public Observable<Session> create() throws URISyntaxException {
        return createSessionRequest()
                .map(sessionResp -> {
                    this.setId(sessionResp.getId());
                    this.setAppId(sessionResp.getAddId());

                    return this;
                });
    }

    @NotNull
    private PostSessions preparePostSessions() {
        PostSessions postBody = new PostSessions();
        postBody.setName(getName());
        postBody.setKind(getKind());

        return postBody;
    }

    private Observable<com.microsoft.azure.hdinsight.sdk.rest.livy.interactive.Session> createSessionRequest()
            throws URISyntaxException {
        URI uri = baseUrl.toURI().resolve(REST_SEGMENT);

        PostSessions postBody = preparePostSessions();

        String json = postBody.convertToJson()
                .orElseThrow(() -> new IllegalArgumentException("Bad session arguments to post."));

        StringEntity entity = new StringEntity(json, Charset.forName("UTF-8"));
        entity.setContentType("application/json");

        return getHttp()
                .post(uri.toString(), entity, null, null)
                .flatMap(HttpObservable::toStringResponse)
                .map(resp -> {
                    try {
                        return ObjectConvertUtils.convertJsonToObject(
                                resp.getMessage(),
                                com.microsoft.azure.hdinsight.sdk.rest.livy.interactive.Session.class)
                                .orElseThrow(() -> propagate(
                                        new HDIException("Unknown Livy server response: " + resp.getMessage())));
                    } catch (IOException e) {
                        throw propagate(e);
                    }
                });
    }

    /**
     * To kill a session, if it's opened, cancel all running statements and close it, otherwise, do nothing
     *
     * @return An updated Session instance Observable
     */
    public Observable<Session> kill() {
        throw new NotImplementedException();
    }

    public Observable<Statement> runStatement(@NotNull Statement statement) {
        throw new NotImplementedException();
    }

    public Observable<Statement> runCodes(@NotNull String codes) {
        throw new NotImplementedException();
    }

    public Observable<String> getLog() {
        throw new NotImplementedException();
    }

}
