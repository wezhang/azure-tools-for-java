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

package com.microsoft.azure.hdinsight.sdk.io.spark;

import com.microsoft.azure.hdinsight.sdk.common.livy.interactive.Session;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class ClusterFileBufferedOutputStream extends OutputStream {
    @NotNull
    private Session session;
    private final String preloadedCodes = "val jarOutput = \"%s\"" +
            "val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)\n" +
            "val jarFileOutput = fs.create(new org.apache.hadoop.fs.Path(jarOutput), true)\n" +
            "val out = new DataOutputStream(new BufferedOutputStream(jarFileOutput))\n" +
            "\n" +
            "import java.io._\n" +
            "import java.util.Base64\n" +
            "\n" +
            "def writePage(encodedBase64: String) = {\n" +
            "    val pageBytes = Base64.getDecoder.decode(encodedBase64)\n" +
            "\n" +
            "    out.write(pageBytes, 0, pageBytes.size)\n" +
            "}\n";


    public ClusterFileBufferedOutputStream(@NotNull Session session, @NotNull URI destination) {
        this.session = session;

        session.runCodes(String.format(preloadedCodes, destination.toString()))
                .subscribe();
    }

    @Override
    public void close() throws IOException {
        session.runCodes("out.close()")
                .toBlocking()
                .single();

        session.close();
        super.close();
    }

    @NotNull
    private StringBuilder bufBuilder = new StringBuilder();

    @Override
    public void write(int b) throws IOException {
        bufBuilder.append(b);
    }

    @Override
    public void flush() throws IOException {
        String codesPage = bufBuilder.toString();
        bufBuilder = new StringBuilder();

        session.runCodes(String.format("writePage(\"%s\")", codesPage))
                .toBlocking()
                .single();

        super.flush();
    }
}
