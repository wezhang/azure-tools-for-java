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

import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.ui.ComboBox
import com.intellij.ui.ComboboxWithBrowseButton
import com.intellij.uiDesigner.core.GridConstraints
import com.intellij.uiDesigner.core.GridConstraints.ANCHOR_WEST
import com.microsoft.azure.hdinsight.common.StreamUtil
import com.microsoft.azure.hdinsight.common.logger.ILogger
import com.microsoft.azure.hdinsight.sdk.common.AzureSparkClusterManager
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitJobUploadStorageModel
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitStorageType
import com.microsoft.azuretools.authmanage.AuthMethodManager
import com.microsoft.azuretools.ijidea.ui.HintTextField
import com.microsoft.intellij.forms.dsl.panel
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import rx.Observable
import rx.schedulers.Schedulers
import java.awt.CardLayout
import java.awt.Dimension
import java.util.stream.Collectors
import javax.swing.ComboBoxModel
import javax.swing.DefaultComboBoxModel
import javax.swing.JLabel
import javax.swing.JPanel

class SparkSubmissionJobUploadStorageAdlsCard: SparkSubmissionJobUploadStorageBasicCard(), ILogger {
    interface Model : SparkSubmissionJobUploadStorageBasicCard.Model {
        var adlsRootPath: String?
        var subscriptionsModel: ComboBoxModel<Any>
        var selectedSubscription: String?
    }

    private val refreshButtonIconPath = "/icons/refresh.png"
    override val title: String = SparkSubmitStorageType.ADLS_GEN1.description
    private val adlsRootPathTip = "e.g. adl://myaccount.azuredatalakestore.net/<root path>"
    private val adlsRootPathLabel = JLabel("ADLS Root Path").apply { toolTipText = adlsRootPathTip }
    val adlsRootPathField = HintTextField(adlsRootPathTip).apply {
        name = "adlsCardRootPathField"
        preferredSize = Dimension(500, 0)
    }
    private val authMethodLabel = JLabel("Authentication Method")
    private val authMethodComboBox = ComboBox<String>(arrayOf("Azure Account")).apply { name = "adlsCardAuthMethodComboBox" }
    private val subscriptionsLabel = JLabel("Subscription List")
    private val subscriptionsComboBox  = ComboboxWithBrowseButton().apply {
        comboBox.name = "adlsCardSubscriptionsComboBoxCombo"
        button.name = "adlsCardSubscriptionsComboBoxButton"
        button.toolTipText = "Refresh"
        button.icon = StreamUtil.getImageResourceFile(refreshButtonIconPath)
        button.addActionListener {
            //refresh subscriptions after refresh button is clicked
            if (button.isEnabled) {
                button.isEnabled = false
                refreshSubscriptions()
                        .doOnEach { button.isEnabled = true }
                        .subscribe(
                                { },
                                { err -> log().warn(ExceptionUtils.getStackTrace(err)) })
            }
        }
    }

    val signInCard = SparkSubmissionJobUploadStorageAdlsSignInCard()
    val signOutCard = SparkSubmissionJobUploadStorageAdlsSignOutCard()
    private val azureAccountCards = JPanel(CardLayout()).apply {
        add(signInCard, signInCard.title)
        add(signOutCard, signOutCard.title)
    }

    init {
        val formBuilder = panel {
            columnTemplate {
                col {
                    anchor = ANCHOR_WEST
                }
                col {
                    anchor = ANCHOR_WEST
                    hSizePolicy = GridConstraints.SIZEPOLICY_WANT_GROW
                    fill = GridConstraints.FILL_HORIZONTAL
                }
            }
            row {
                c(adlsRootPathLabel); c(adlsRootPathField)
            }
            row {
                c(authMethodLabel); c(authMethodComboBox)
            }
            row {
                c(); c(azureAccountCards)
            }
            row {
                c(subscriptionsLabel); c(subscriptionsComboBox)
            }
        }

        layout = formBuilder.createGridLayoutManager()
        formBuilder.allComponentConstraints.forEach { (component, gridConstrains) -> add(component, gridConstrains) }
    }

    private fun refreshSubscriptions(): Observable<SparkSubmitJobUploadStorageModel> {
        ApplicationManager.getApplication().assertIsDispatchThread()

        return Observable.just(SparkSubmitJobUploadStorageModel())
                .doOnNext { getData(it) }
                // set error message to prevent user from applying the change when refreshing is not completed
                .map { it.apply { errorMsg = "refreshing subscriptions is not completed" } }
                .doOnNext { setData(it) }
                .observeOn(Schedulers.io())
                .map { toUpdate ->
                    toUpdate.apply {
                        if (!AzureSparkClusterManager.getInstance().isSignedIn) {
                            errorMsg = "ADLS Gen 1 storage type requires user to sign in first"
                        } else {
                            try {
                                val subscriptionManager = AuthMethodManager.getInstance().azureManager.subscriptionManager
                                val subscriptionNameList = subscriptionManager.selectedSubscriptionDetails
                                        .stream()
                                        .map { subDetail -> subDetail.subscriptionName }
                                        .sorted()
                                        .collect(Collectors.toList<String>())

                                if (subscriptionNameList.size > 0) {
                                    subscriptionsModel = DefaultComboBoxModel(subscriptionNameList.toTypedArray())
                                    subscriptionsModel.selectedItem = subscriptionsModel.getElementAt(0)
                                    selectedSubscription = subscriptionsModel.getElementAt(0)?.toString()
                                    errorMsg = null
                                } else {
                                    errorMsg = "No subscriptions found in this storage account"
                                }
                            } catch (ex: Exception) {
                                log().info("Refresh subscriptions error. " + ExceptionUtils.getStackTrace(ex))
                                errorMsg = "Can't get subscriptions, check if subscriptions selected"
                            }
                        }
                    }
                }
                .doOnNext { data ->
                    if (data.errorMsg != null) {
                        log().info("Refresh subscriptions error: " + data.errorMsg)
                    }
                    setData(data)
                }
    }

    override fun readWithLock(to: SparkSubmissionJobUploadStorageBasicCard.Model) {
        if (to !is Model) {
            return
        }

        to.adlsRootPath = adlsRootPathField.text?.trim()
        to.subscriptionsModel = subscriptionsComboBox.comboBox.model
        to.selectedSubscription = subscriptionsComboBox.comboBox.selectedItem?.toString()
    }

    override fun writeWithLock(from: SparkSubmissionJobUploadStorageBasicCard.Model) {
        if (from !is Model) {
            return
        }

        // Only set for changed
        if (adlsRootPathField.text != from.adlsRootPath) {
            adlsRootPathField.text = from.adlsRootPath
        }

        // show sign in/out panel based on whether user has signed in or not
        val curLayout = azureAccountCards.layout as CardLayout
        if (AzureSparkClusterManager.getInstance().isSignedIn) {
            curLayout.show(azureAccountCards, signOutCard.title)
            signOutCard.azureAccountLabel.text = AzureSparkClusterManager.getInstance().azureAccountEmail
        } else {
            curLayout.show(azureAccountCards, signInCard.title)
        }

        if (from.subscriptionsModel.size == 0
                && StringUtils.isBlank(from.errorMsg)
                && StringUtils.isNotEmpty(from.selectedSubscription)) {
            subscriptionsComboBox.comboBox.model = DefaultComboBoxModel(arrayOf(from.selectedSubscription))
        } else {
            subscriptionsComboBox.comboBox.model = from.subscriptionsModel
        }
    }

}