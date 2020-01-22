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

import com.intellij.ui.components.fields.ExpandableTextField
import com.intellij.uiDesigner.core.GridConstraints
import com.intellij.uiDesigner.core.GridConstraints.ANCHOR_WEST
import com.microsoft.azure.hdinsight.common.AbfsUri
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitStorageType
import com.microsoft.azure.hdinsight.spark.common.getSecureStoreServiceOf
import com.microsoft.azuretools.ijidea.ui.HintTextField
import com.microsoft.azuretools.securestore.SecureStore
import com.microsoft.azuretools.service.ServiceManager
import com.microsoft.intellij.forms.dsl.panel
import org.apache.commons.lang3.StringUtils
import java.awt.Dimension
import javax.swing.JLabel

class SparkSubmissionJobUploadStorageGen2Card : SparkSubmissionJobUploadStorageBasicCard() {
    interface Model: SparkSubmissionJobUploadStorageBasicCard.Model {
        var gen2RootPath: AbfsUri?
        var gen2Account: String?
        var accessKey: String?
    }

    private val secureStore: SecureStore? = ServiceManager.getServiceProvider(SecureStore::class.java)
    private val refreshButtonIconPath = "/icons/refresh.png"
    private val storageKeyTip = "The access key of the default storage account, which can be found from HDInsight cluster storage accounts of Azure portal."
    private val storageKeyLabel = JLabel("Access Key").apply { toolTipText = storageKeyTip }
    val storageKeyField = ExpandableTextField().apply { toolTipText = storageKeyTip; name = "gen2CardstorageKeyField" }
    private val gen2RootPathTip = "e.g. abfs://<file_system>@<account_name>.dfs.core.windows.net/<path>"
    private val gen2RootPathLabel = JLabel("ADLS GEN2 Root Path")
    val gen2RootPathField = HintTextField (gen2RootPathTip).apply {
        name = "gen2CardRootPathField"
        preferredSize = Dimension(500, 0)
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
                c(gen2RootPathLabel.apply { labelFor = gen2RootPathField });    c(gen2RootPathField)
            }
            row {
                c(storageKeyLabel.apply { labelFor = storageKeyField });        c(storageKeyField)
            }
        }

        layout = formBuilder.createGridLayoutManager()
        formBuilder.allComponentConstraints.forEach { (component, gridConstrains) -> add(component, gridConstrains) }
    }

    override val title = SparkSubmitStorageType.ADLS_GEN2.description
    override fun readWithLock(to: SparkSubmissionJobUploadStorageBasicCard.Model) {
        if (to !is Model) {
            return
        }

        val rootPathText = gen2RootPathField.text?.trim()
        to.gen2RootPath = if (AbfsUri.isType(rootPathText)) AbfsUri.parse(rootPathText) else null
        to.gen2Account = to.gen2RootPath?.accountName
        to.accessKey = storageKeyField.text?.trim()
    }

    override fun writeWithLock(from: SparkSubmissionJobUploadStorageBasicCard.Model) {
        if (from !is Model) {
            return
        }

        if (from.gen2RootPath?.toString() != gen2RootPathField.text.trim()) {
            gen2RootPathField.text = from.gen2RootPath?.uri?.toString() ?: ""
        }

        val credentialAccount = SparkSubmitStorageType.ADLS_GEN2.getSecureStoreServiceOf(from.gen2RootPath?.accountName)
        storageKeyField.text =
                if (StringUtils.isBlank(from.accessKey)) {
                    credentialAccount?.let { secureStore?.loadPassword(credentialAccount, from.gen2Account) ?: "" }
                } else {
                    from.accessKey
                }
    }
}