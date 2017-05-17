/**
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

package com.microsoft.azure.hdinsight.spark.common;

import java.io.IOException;
import java.net.URISyntaxException;

public interface ISparkBatchDebugJob {
    /**
     * Create a batch Spark job with driver debugging enabled
     *
     * @return the current instance for chain calling
     * @throws IOException the exceptions for networking connection issues related
     * @throws DebugParameterDefinedException the exception for debug option already defined in Spark submission parameter
     */
    public ISparkBatchDebugJob createBatchSparkJobWithDriverDebugging() throws IOException, DebugParameterDefinedException;

    /**
     * Kill the batch job specified by ID
     *
     * @return the current instance for chain calling
     * @throws IOException exceptions for networking connection issues related
     */
    public ISparkBatchDebugJob killBatchJob() throws IOException;

    /**
     * Get Spark Batch job driver debugging port number
     *
     * @return Spark driver node debugging port
     * @throws IOException exceptions for the driver debugging port not found
     */
    public int getSparkDriverDebuggingPort() throws IOException;

    /**
     * Get Spark batch job driver host by ID
     *
     * @return Spark driver node host
     * @throws IOException exceptions for the driver host not found
     */
    public String getSparkDriverHost() throws IOException, URISyntaxException;
}
