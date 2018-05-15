/*
 * Copyright (c) Microsoft Corporation
 *
 * All rights reserved.
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 * THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.hdinsight.sdk.cluster;

import com.google.common.collect.ImmutableList;
import com.microsoft.azure.hdinsight.sdk.common.HDIException;
import com.microsoft.azure.hdinsight.sdk.storage.HDStorageAccount;
import com.microsoft.azure.hdinsight.sdk.storage.IHDIStorageAccount;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;

import java.util.List;

public abstract class SparkCluster implements IClusterDetail {
    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getTitle() {
        return null;
    }

    @Override
    public String getState() {
        return null;
    }

    @Override
    public String getLocation() {
        return null;
    }

    @Override
    public String getConnectionUrl() {
        return null;
    }

    @Override
    public String getCreateDate() {
        return null;
    }

    @Override
    public ClusterType getType() {
        return ClusterType.spark;
    }

    @Override
    public String getVersion() {
        return null;
    }

    @Override
    public int getDataNodes() {
        return 0;
    }

    @Override
    public String getOSType() {
        return null;
    }

    @Override
    public String getResourceGroup() {
        return null;
    }

    @Nullable
    @Override
    public IHDIStorageAccount getStorageAccount() throws HDIException {
        return null;
    }

    @Override
    public List<HDStorageAccount> getAdditionalStorageAccounts() {
        return ImmutableList.of();
    }

    @Override
    public String getSparkVersion() {
        return null;
    }
}
