/**
 * Copyright (c) Microsoft Corporation
 *
 *
 * All rights reserved.
 *
 *
 * MIT License
 *
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 *
 * THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.hdinsight.spark.ui

import com.microsoft.azure.hdinsight.common.AbfsUri
import com.microsoft.azure.hdinsight.common.logger.ILogger
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail
import com.microsoft.azure.hdinsight.sdk.common.azure.serverless.AzureSparkCosmosCluster
import com.microsoft.azure.hdinsight.sdk.storage.ADLSGen2StorageAccount
import com.microsoft.azure.hdinsight.sdk.storage.ADLSStorageAccount
import com.microsoft.azure.hdinsight.sdk.storage.HDStorageAccount
import com.microsoft.azure.hdinsight.sdk.storage.IHDIStorageAccount
import org.apache.commons.lang3.StringUtils
import java.awt.event.FocusAdapter
import java.awt.event.FocusEvent

class SparkSubmissionJobUploadStorageCtrl(val view: SparkSubmissionJobUploadStorageWithUploadPathPanel) :
        SparkSubmissionJobUploadStorageWithUploadPathPanel.Control, ILogger {
    override val isCheckPassed
        get() = StringUtils.isEmpty(resultMessage)
    override val resultMessage
        get() = view.storagePanel.errorMessage

    //storage check event for storageCheckSubject in panel
    open class StorageCheckEvent(val message: String)
    class StorageCheckSelectedClusterEvent(val cluster: IClusterDetail,val preClusterName: String?) : StorageCheckEvent(
            "Selected cluster ${cluster.name} with preClusterName $preClusterName")
    class StorageCheckSignInOutEvent() : StorageCheckEvent("After user clicked sign in/off in ADLS Gen 1 storage type")
    class StorageCheckPathFocusLostEvent(val rootPathType: String) : StorageCheckEvent("$rootPathType root path focus lost")
    class StorageCheckSelectedStorageTypeEvent(val storageType: String) : StorageCheckEvent("Selected storage type: $storageType")

    init {
        arrayOf(view.storagePanel.adlsGen2Card.gen2RootPathField, view.storagePanel.adlsGen2OAuthCard.gen2RootPathField)
            .forEach {
                it.addFocusListener(object : FocusAdapter() {
                    override fun focusLost(e: FocusEvent?) {
                        view.viewModel.uploadStorage.storageCheckSubject.onNext(StorageCheckPathFocusLostEvent("ADLS GEN2"))
                    }
                })
            }
    }

    override fun getAzureBlobStoragePath(fullStorageBlobName: String?, container: String?, schema: String): String? {
        if (StringUtils.isBlank(fullStorageBlobName) || StringUtils.isBlank(container) || schema.isBlank())
            throw IllegalArgumentException("Blob Name ,container and scheme name cannot be empty")

        val rawStoragePath = "$schema://$container@$fullStorageBlobName"
        return if (schema.startsWith(ADLSGen2StorageAccount.DefaultScheme))
            AbfsUri.parse("$rawStoragePath/${SparkSubmissionContentPanel.Constants.submissionFolder}/").url.toString()
        else "$rawStoragePath/${SparkSubmissionContentPanel.Constants.submissionFolder}/"
    }

    override fun getUploadPath(account: IHDIStorageAccount): String? =
            when (account) {
                is HDStorageAccount -> getAzureBlobStoragePath(account.fullStorageBlobName, account.defaultContainer, account.scheme)
                is ADLSStorageAccount ->
                    if (StringUtils.isBlank(account.name) || StringUtils.isBlank(account.defaultContainerOrRootPath)) null
                    else "adl://${account.name}.azuredatalakestore.net${account.defaultContainerOrRootPath + SparkSubmissionContentPanel.Constants.submissionFolder}/"
                is AzureSparkCosmosCluster.StorageAccount -> account.defaultContainerOrRootPath?.let { "${it + SparkSubmissionContentPanel.Constants.submissionFolder}/" }
                else -> null
            }
}