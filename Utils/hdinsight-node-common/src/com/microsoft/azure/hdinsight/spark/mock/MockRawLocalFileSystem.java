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

package com.microsoft.azure.hdinsight.spark.mock;


import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class MockRawLocalFileSystem extends RawLocalFileSystem {
    @Override
    public URI getUri() {
        try {
            return new URI("mockfs:///");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Path getWorkingDirectory() {
        Path workingDir = new Path("/user/current");

        // Default URI is wasb:///
        return workingDir.makeQualified(URI.create("wasb:///"), workingDir);
    }

    @Override
    protected void checkPath(Path path) { }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        FileStatus result = super.getFileStatus(f);
        result.setPath(f);

        return result;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        File localf = pathToFile(f);
        FileStatus[] results;
        results = super.listStatus(f);

        if (localf.isFile() && results.length > 0) {
            results[0].setPath(f);
        }

        return results;
    }

    @Override
    public File pathToFile(Path path) {
        @NotNull
        Path realPath;

        URI originUri = path.toUri();

        if (originUri.getScheme() != null &&
                (originUri.getScheme().toLowerCase().equals("file") ||
                 originUri.getScheme().toLowerCase().equals("mockfs"))) {
            realPath = path;
        } else {
            if (!path.isAbsolute()) {
                path = new Path(getWorkingDirectory(), path);
            }

            List<String> components = Arrays.stream(path.toUri().getPath().split(Path.SEPARATOR))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    // skip the driver name, like C:, D:
                    .skip((Path.WINDOWS && Path.isWindowsAbsolutePath(path.toUri().getPath(), true)) ? 1 : 0)
                    .collect(Collectors.toList());

            Path fsRoot = Optional.ofNullable(originUri.getAuthority())
                    .filter(auth -> !auth.isEmpty())
                    .map(auth -> new Path(new Path(System.getProperty("user.dir")).getParent().getParent().getParent(), auth))
                    .orElse(new Path(System.getProperty("user.dir")).getParent().getParent());

            realPath = new Path(fsRoot, String.join(Path.SEPARATOR, components));
        }

        return new File(realPath.toUri().getPath());
    }
}