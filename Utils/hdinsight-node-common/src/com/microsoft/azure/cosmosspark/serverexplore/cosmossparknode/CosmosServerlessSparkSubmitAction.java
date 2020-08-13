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

package com.microsoft.azure.cosmosspark.serverexplore.cosmossparknode;

import com.microsoft.azure.hdinsight.sdk.common.azure.serverless.AzureSparkServerlessAccount;
import com.microsoft.azuretools.azurecommons.helpers.AzureCmdException;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.tooling.msservices.serviceexplorer.NodeActionEvent;
import com.microsoft.tooling.msservices.serviceexplorer.NodeActionListener;
import org.apache.commons.lang3.tuple.Pair;
import rx.subjects.PublishSubject;

public class CosmosServerlessSparkSubmitAction extends NodeActionListener {
    @NotNull
    private final AzureSparkServerlessAccount adlAccount;
    @NotNull
    private final PublishSubject<Pair<AzureSparkServerlessAccount, CosmosSparkADLAccountNode>> provisionAction;
    @NotNull
    private final CosmosSparkADLAccountNode adlAccountNode;

    public CosmosServerlessSparkSubmitAction(@NotNull CosmosSparkADLAccountNode adlAccountNode,
                                             @NotNull AzureSparkServerlessAccount adlAccount,
                                             @NotNull PublishSubject<Pair<
                                                  AzureSparkServerlessAccount,
                                                  CosmosSparkADLAccountNode>> provisionAction) {
        super();
        this.adlAccount = adlAccount;
        this.provisionAction = provisionAction;
        this.adlAccountNode = adlAccountNode;
    }

    @Override
    protected void actionPerformed(NodeActionEvent e) {
        provisionAction.onNext(Pair.of(adlAccount, adlAccountNode));
    }
}
