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

package com.microsoft.azure.hdinsight.spark.ui

import com.intellij.openapi.Disposable
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.application.ModalityState
import com.intellij.openapi.util.Disposer
import com.intellij.uiDesigner.core.GridConstraints.*
import com.microsoft.azure.hdinsight.common.*
import com.microsoft.azure.hdinsight.common.logger.ILogger
import com.microsoft.azure.hdinsight.common.mvc.IdeaSettableControlWithRwLock
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail
import com.microsoft.azure.hdinsight.sdk.cluster.MfaEspCluster
import com.microsoft.azure.hdinsight.sdk.common.AzureSparkClusterManager
import com.microsoft.azure.hdinsight.sdk.common.azure.serverless.AzureSparkServerlessAccount
import com.microsoft.azure.hdinsight.sdk.storage.HDStorageAccount
import com.microsoft.azure.hdinsight.sdk.storage.IHDIStorageAccount
import com.microsoft.azure.hdinsight.sdk.storage.StorageAccountType
import com.microsoft.azure.hdinsight.spark.common.SparkBatchJob
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitJobUploadStorageModel
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitStorageType
import com.microsoft.azure.hdinsight.spark.common.getSecureStoreServiceOf
import com.microsoft.azure.hdinsight.spark.ui.SparkSubmissionJobUploadStorageCtrl.*
import com.microsoft.azure.sqlbigdata.sdk.cluster.SqlBigDataLivyLinkClusterDetail
import com.microsoft.azuretools.authmanage.AuthMethodManager
import com.microsoft.azuretools.ijidea.ui.AccessibleHideableTitledPanel
import com.microsoft.azuretools.securestore.SecureStore
import com.microsoft.azuretools.service.ServiceManager
import com.microsoft.intellij.forms.dsl.panel
import com.microsoft.intellij.rxjava.DisposableObservers
import com.microsoft.intellij.rxjava.IdeaSchedulers
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import rx.Observable
import rx.Observable.empty
import rx.Observable.just
import rx.subjects.ReplaySubject
import java.awt.CardLayout
import java.util.concurrent.TimeUnit
import javax.swing.DefaultComboBoxModel
import javax.swing.JLabel
import javax.swing.JPanel
import javax.swing.JTextField

class SparkSubmissionJobUploadStorageWithUploadPathPanel
    : JPanel(), Disposable, IdeaSettableControlWithRwLock<SparkSubmitJobUploadStorageModel>, ILogger {
    interface Control {
        val isCheckPassed: Boolean
        val resultMessage: String?
        fun getUploadPath(account: IHDIStorageAccount): String?
        fun getAzureBlobStoragePath(fullStorageBlobName: String?, container: String?, schema: String): String?
    }

    private val secureStore: SecureStore? = ServiceManager.getServiceProvider(SecureStore::class.java)
    private val jobUploadStorageTitle = "Job Upload Storage"
    private val invalidUploadPath = "<Invalid Upload Path>"
    private val unsupportAccountType = "<Storage Account Type Is Not Supported>"
    private val uploadPathLabel = JLabel("Upload Path")
    private val uploadPathField = JTextField().apply {
        isEditable = false
    }

    val storagePanel = SparkSubmissionJobUploadStoragePanel().apply {
        Disposer.register(this@SparkSubmissionJobUploadStorageWithUploadPathPanel, this@apply)
    }

    private val hideableJobUploadStoragePanel = AccessibleHideableTitledPanel(jobUploadStorageTitle, storagePanel)

    init {
        val formBuilder = panel {
            columnTemplate {
                col {
                    anchor = ANCHOR_WEST
                }
                col {
                    anchor = ANCHOR_WEST
                    hSizePolicy = SIZEPOLICY_WANT_GROW
                    fill = FILL_HORIZONTAL
                }
                row {
                    c(uploadPathLabel
                            .apply { labelFor = uploadPathField }) { indent = 0 }
                                                                                        c(uploadPathField) {}
                }
                row {
                    c(hideableJobUploadStoragePanel) { colSpan = 2; hSizePolicy = SIZEPOLICY_WANT_GROW; fill = FILL_HORIZONTAL }
                }
            }
        }

        layout = formBuilder.createGridLayoutManager()
        formBuilder.allComponentConstraints.forEach { (component, gridConstrains) -> add(component, gridConstrains) }
    }

    val control: Control = SparkSubmissionJobUploadStorageCtrl(this)

    inner class ViewModel : DisposableObservers() {
        val uploadStorage = storagePanel.viewModel.apply {
            // check storage info when cluster selection changes
            storageCheckSubject
                    .groupBy { checkEvent -> checkEvent::class.java.typeName}
                    .subscribe(
                            { groupedOb ->
                                groupedOb
                                        .throttleWithTimeout(200, TimeUnit.MILLISECONDS)
                                        .doOnNext { log().info("Receive checking message ${it.message}") }
                                        .flatMap { validateStorageInfo(it) }
                                        .subscribe()
                            },
                            { err -> log().warn(ExceptionUtils.getStackTrace(err)) })
        }

        val clusterSelectedCapacity = 2

        //in order to get the pre select cluster name, use replaysubject type
        val clusterSelectedSubject: ReplaySubject<IClusterDetail> = disposableSubjectOf {
            ReplaySubject.createWithSize(clusterSelectedCapacity)
        }

        private val ideaSchedulers = IdeaSchedulers()

        private fun updateStorageTypesModelForCluster(clusterDetail: IClusterDetail): SparkSubmitStorageType? {
            ApplicationManager.getApplication().assertIsDispatchThread()

            val optionTypes = clusterDetail.storageOptionsType.optionTypes
            val currentStorageTypesModel = uploadStorage.deployStorageTypesModel
            val newStorageTypesModel = ImmutableComboBoxModel(optionTypes)

            val storageTypesModelToSet = if (currentStorageTypesModel.size == optionTypes.size) {
                // Deep compare the size and items
                var isDeepEqualed = true
                for (i in 0 until currentStorageTypesModel.size) {
                    if (currentStorageTypesModel.getElementAt(i) != optionTypes[i]) {
                        isDeepEqualed = false
                        break;
                    }
                }

                if (isDeepEqualed) currentStorageTypesModel else newStorageTypesModel
            } else newStorageTypesModel

            if (storageTypesModelToSet != currentStorageTypesModel) {
                // there exist 4 cases to set the storage type
                // 1.select cluster -> set to default
                // 2.reload config with not null type -> set to saved type
                // 3.reload config with null storage type -> set to default
                // 4.create config  -> set to default
                uploadStorage.deployStorageTypesModel = storageTypesModelToSet
                uploadStorage.deployStorageTypeSelection = clusterDetail.defaultStorageType
            }

            return storageTypesModelToSet.selectedItem as? SparkSubmitStorageType ?: clusterDetail.defaultStorageType
        }

        private fun validateStorageInfo(checkEvent:StorageCheckEvent): Observable<SparkSubmitJobUploadStorageModel> {
            val cluster = clusterSelectedSubject.value ?: return empty()
            val modalityState = ModalityState.stateForComponent(this@SparkSubmissionJobUploadStorageWithUploadPathPanel)

            return just(SparkSubmitJobUploadStorageModel()
                    .apply {
                        // Hacking for ADLS Gen2 cluster storage account name
                        gen2Account = cluster.storageAccount?.name.takeIf {
                            cluster.storageAccount?.accountType == StorageAccountType.ADLSGen2
                        }
                    })
                    .doOnNext { model -> getData(model) }
                    .observeOn(ideaSchedulers.dispatchUIThread(modalityState))
                    .doOnNext { model -> model.apply {
                        if (checkEvent is StorageCheckSelectedClusterEvent) {
                            log().info("Current model storage account type is $storageAccountType")
                            storageAccountType = updateStorageTypesModelForCluster(cluster)
                            log().info("Update model storage account type to $storageAccountType")
                        }
                        // set error message to prevent user from applying the changes when validation is not completed
                        errorMsg = "validating storage info is not completed"
                    }}
                    .observeOn(ideaSchedulers.dispatchPooledThread())
                    .map { model ->
                        model.errorMsg = null
                        model.uploadPath = invalidUploadPath

                        when (model.storageAccountType) {
                            SparkSubmitStorageType.SPARK_INTERACTIVE_SESSION -> model.apply {
                                uploadPath = "/SparkSubmission/"
                            }
                            SparkSubmitStorageType.DEFAULT_STORAGE_ACCOUNT -> model.apply {
                                try {
                                    cluster.getConfigurationInfo()
                                    val defaultStorageAccount = cluster.storageAccount
                                    if (defaultStorageAccount == null) {
                                        errorMsg = "Cluster have no storage account"
                                        return@apply
                                    }

                                    // TODO: need to test whether this has block issue
                                    // Here is a bug before this commit. If cluster.defaultStorageRootPath is not
                                    // null, the path will not add "/SparkSubmission" as suffix. This is because
                                    // operator + is calculated ahead of operator ?:
                                    // To address issue https://github.com/microsoft/azure-tools-for-java/issues/3856
                                    val path = (cluster.defaultStorageRootPath?.trimEnd('/') ?: "") +
                                            "/${SparkSubmissionContentPanel.Constants.submissionFolder}/"

                                    uploadPath = if (AbfsUri.isType(path)) AbfsUri.parse(path).uri.toString() else path
                                } catch (ex: Exception) {
                                    errorMsg = "Error getting cluster storage configuration"
                                    log().warn(errorMsg + ". " + ExceptionUtils.getStackTrace(ex))
                                }
                            }
                            SparkSubmitStorageType.BLOB -> model.apply {
                                if (containersModel.size == 0
                                        || containersModel.selectedItem == null
                                        || storageAccount.isNullOrBlank()
                                        || storageKey.isNullOrBlank()) {
                                    errorMsg = "Azure Blob storage form is not completed"
                                    return@apply
                                }

                                uploadPath = control.getAzureBlobStoragePath(
                                        ClusterManagerEx.getInstance().getBlobFullName(storageAccount),
                                        containersModel.selectedItem as String,
                                        HDStorageAccount.DefaultScheme)
                            }
                            SparkSubmitStorageType.ADLS_GEN1 -> model.apply {
                                if (!AzureSparkClusterManager.getInstance().isSignedIn) {
                                    errorMsg = "ADLS Gen 1 storage type requires user to sign in first"
                                    return@apply
                                } else if (adlsRootPath != null && !AdlUri.isType(adlsRootPath)) {
                                    // basic validation for ADLS root path
                                    // pattern for adl root path. e.g. adl://john.azuredatalakestore.net/root/path/
                                    errorMsg = "ADLS Root Path is invalid"
                                    return@apply
                                }

                                val adlUri = AdlUri.parse(adlsRootPath)
                                uploadPath = adlUri.resolveAsRoot("SparkSubmission/").toString()
                            }
                            SparkSubmitStorageType.ADLS_GEN2, SparkSubmitStorageType.ADLS_GEN2_FOR_OAUTH -> model.apply {
                                val rootPath = gen2RootPath
                                if (rootPath == null) {
                                    errorMsg = "ADLS GEN2 Root Path is invalid"
                                    return@apply
                                } else if (this.storageAccountType == SparkSubmitStorageType.ADLS_GEN2_FOR_OAUTH
                                        && !AuthMethodManager.getInstance().isSignedIn) {
                                    errorMsg = "Need to use azure account to login in first"
                                    return@apply
                                }

                                val homeUri = if (cluster is MfaEspCluster) rootPath.resolveAsRoot(cluster.userPath) else rootPath
                                uploadPath = homeUri.resolveAsRoot("SparkSubmission/").toString()
                            }
                            SparkSubmitStorageType.WEBHDFS -> model.apply {
                                //pattern for webhdfs root path.e.g http://host/webhdfs/v1/
                                val rootPath = webHdfsRootPath?.trim() ?: return@apply
                                if (!SparkBatchJob.WebHDFSPathPattern.toRegex().matches(rootPath)) {
                                    errorMsg = "Webhdfs root path is not valid"
                                    return@apply
                                }

                                val formatWebHdfsRootPath = if (rootPath.endsWith("/")) rootPath.trimEnd('/') else rootPath
                                uploadPath = "$formatWebHdfsRootPath/SparkSubmission/"
                                webHdfsAuthUser = when (cluster) {
                                    is SqlBigDataLivyLinkClusterDetail -> cluster.httpUserName
                                    else -> SparkSubmissionJobUploadWebHdfsSignOutCard.defaultAuthUser
                                }
                            }
                            SparkSubmitStorageType.ADLA_ACCOUNT_DEFAULT_STORAGE -> model.apply {
                                val account = cluster as? AzureSparkServerlessAccount
                                if (account == null) {
                                    errorMsg = "Selected ADLA account does not exist"
                                    return@apply
                                }

                                uploadPath = "${account.storageRootPath}SparkSubmission/"
                            }
                            SparkSubmitStorageType.NOT_SUPPORT_STORAGE_TYPE -> model.apply {
                                errorMsg = "Storage type is not supported"
                            }
                            else -> model.apply {
                                errorMsg = "Storage type is undefined"
                            }
                        }
                    }
                    .observeOn(ideaSchedulers.dispatchUIThread(modalityState))
                    .doOnNext { data ->
                        if (data.errorMsg != null) {
                            log().info("After selecting storage type, the storage info validation error is got: " + data.errorMsg)
                        }
                        setData(data)
                    }
        }

        fun getCurrentUploadFieldText() : String? = uploadPathField.text?.trim()
    }

    val viewModel = ViewModel().apply {
        Disposer.register(this@SparkSubmissionJobUploadStorageWithUploadPathPanel, this@apply)
    }

    override fun readWithLock(data: SparkSubmitJobUploadStorageModel) {
        // Component -> Data
        data.errorMsg = storagePanel.errorMessage
        data.uploadPath = uploadPathField.text
        data.storageAccountType = viewModel.uploadStorage.deployStorageTypesModel.selectedItem as? SparkSubmitStorageType

        when (viewModel.uploadStorage.deployStorageTypeSelection) {
            SparkSubmitStorageType.BLOB -> {
                data.storageAccount = storagePanel.azureBlobCard.storageAccountField.text.trim()
                data.storageKey = storagePanel.azureBlobCard.storageKeyField.text.trim()
                data.containersModel = storagePanel.azureBlobCard.storageContainerUI.comboBox.model
                data.selectedContainer = storagePanel.azureBlobCard.storageContainerUI.comboBox.selectedItem?.toString()
            }
            SparkSubmitStorageType.ADLS_GEN1 -> {
                data.adlsRootPath = storagePanel.adlsCard.adlsRootPathField.text.trim()
                data.subscriptionsModel = storagePanel.adlsCard.subscriptionsComboBox.comboBox.model
                data.selectedSubscription = storagePanel.adlsCard.subscriptionsComboBox.comboBox.selectedItem?.toString()
            }
            SparkSubmitStorageType.WEBHDFS -> {
                data.webHdfsRootPath= storagePanel.webHdfsCard.webHdfsRootPathField.text.trim()
            }
            SparkSubmitStorageType.ADLS_GEN2 -> {
                val rootPathText = storagePanel.adlsGen2Card.gen2RootPathField.text.trim()
                data.gen2RootPath = if (AbfsUri.isType(rootPathText)) AbfsUri.parse(rootPathText) else null
                data.gen2Account = data.gen2RootPath?.accountName ?: ""
                data.accessKey = storagePanel.adlsGen2Card.storageKeyField.text.trim()
            }
            SparkSubmitStorageType.ADLS_GEN2_FOR_OAUTH -> {
                val rootPathText = storagePanel.adlsGen2OAuthCard.gen2RootPathField.text.trim()
                data.gen2RootPath = if (AbfsUri.isType(rootPathText)) AbfsUri.parse(rootPathText) else null
            }
            else -> {}
        }
    }

    override fun writeWithLock(data: SparkSubmitJobUploadStorageModel) {
        viewModel.uploadStorage.apply {
            if (deployStorageTypeSelection != data.storageAccountType) {
                if (deployStorageTypesModel.size == 0) {
                    deployStorageTypesModel = ImmutableComboBoxModel(
                            data.storageAccountType?.let { arrayOf(it) } ?: arrayOf())
                }

                deployStorageTypeSelection = data.storageAccountType
            }
        }

        storagePanel.errorMessage = data.errorMsg
        uploadPathField.text = data.uploadPath
        when (data.storageAccountType) {
            SparkSubmitStorageType.BLOB -> storagePanel.azureBlobCard.apply {
                if (storageAccountField.text != data.storageAccount) {
                    storageAccountField.text = data.storageAccount
                }

                val credentialAccount = SparkSubmitStorageType.BLOB.getSecureStoreServiceOf(data.storageAccount)
                val storageKeyToSet =
                        if (StringUtils.isEmpty(data.errorMsg) && StringUtils.isEmpty(data.storageKey)) {
                            credentialAccount?.let { secureStore?.loadPassword(credentialAccount, data.storageAccount) }
                        } else {
                            data.storageKey
                        }

                if (storageKeyField.text != storageKeyToSet) {
                    storageKeyField.text = storageKeyToSet
                }

                if (storageContainerUI.comboBox.model != data.containersModel) {
                    if (data.containersModel.size == 0
                            && StringUtils.isEmpty(storagePanel.errorMessage)
                            && StringUtils.isNotEmpty(data.selectedContainer)) {
                        storageContainerUI.comboBox.model = DefaultComboBoxModel(arrayOf(data.selectedContainer))
                    } else {
                        storageContainerUI.comboBox.model = data.containersModel
                    }
                }
            }
            SparkSubmitStorageType.ADLS_GEN1 -> storagePanel.adlsCard.apply {
                // Only set for changed
                if (adlsRootPathField.text != data.adlsRootPath) {
                    adlsRootPathField.text = data.adlsRootPath
                }

                // show sign in/out panel based on whether user has signed in or not
                val curLayout = azureAccountCards.layout as CardLayout
                if (AzureSparkClusterManager.getInstance().isSignedIn) {
                    curLayout.show(azureAccountCards, storagePanel.adlsCard.signOutCard.title)
                    signOutCard.azureAccountLabel.text = AzureSparkClusterManager.getInstance().azureAccountEmail
                } else {
                    curLayout.show(azureAccountCards, signInCard.title)
                }

                if (data.subscriptionsModel.size == 0
                        && StringUtils.isEmpty(storagePanel.errorMessage)
                        && StringUtils.isNotEmpty(data.selectedSubscription)) {
                    subscriptionsComboBox.comboBox.model = DefaultComboBoxModel(arrayOf(data.selectedSubscription))
                } else {
                    subscriptionsComboBox.comboBox.model = data.subscriptionsModel
                }
            }
            SparkSubmitStorageType.WEBHDFS -> storagePanel.webHdfsCard.apply {
                if (webHdfsRootPathField.text != data.webHdfsRootPath) {
                    webHdfsRootPathField.text = data.webHdfsRootPath
                }

                // show sign in/out panel based on whether user has signed in or not
                val curLayout = authAccountForWebHdfsCards.layout as CardLayout
                curLayout.show(authAccountForWebHdfsCards, signOutCard.title)
                signOutCard.authUserNameLabel.text = data.webHdfsAuthUser
            }
            SparkSubmitStorageType.ADLS_GEN2 -> storagePanel.adlsGen2Card.apply {
                if (data.gen2RootPath?.toString() != gen2RootPathField.text.trim()) {
                    gen2RootPathField.text = data.gen2RootPath?.uri?.toString() ?: ""
                }

                val credentialAccount = SparkSubmitStorageType.ADLS_GEN2.getSecureStoreServiceOf(data.gen2RootPath?.accountName)
                storageKeyField.text =
                        if (StringUtils.isEmpty(data.accessKey)) {
                            credentialAccount?.let { secureStore?.loadPassword(credentialAccount, data.gen2Account) ?: "" }
                        } else {
                            data.accessKey
                        }
            }
            SparkSubmitStorageType.ADLS_GEN2_FOR_OAUTH -> storagePanel.adlsGen2OAuthCard.apply {
                val gen2PathText = gen2RootPathField.text.trim()
                val parsedGen2Path = if (AbfsUri.isType(gen2PathText)) AbfsUri.parse(gen2PathText) else null
                if (data.gen2RootPath != parsedGen2Path) {
                    gen2RootPathField.text = data.gen2RootPath?.uri?.toString() ?: ""
                }
            }
            else -> { }
        }
    }

    override fun dispose() {
    }
}