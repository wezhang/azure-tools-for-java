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
import com.intellij.openapi.util.Disposer
import com.intellij.uiDesigner.core.GridConstraints.*
import com.microsoft.azure.hdinsight.common.logger.ILogger
import com.microsoft.azure.hdinsight.common.mvc.IdeaSettableControlWithRwLock
import com.microsoft.azure.hdinsight.common.mvvm.Mvvm
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitJobUploadStorageModel
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitStorageType
import com.microsoft.azuretools.ijidea.ui.AccessibleHideableTitledPanel
import com.microsoft.intellij.forms.dsl.panel
import com.microsoft.intellij.rxjava.DisposableObservers
import com.microsoft.intellij.rxjava.IdeaSchedulers
import javax.swing.JLabel
import javax.swing.JTextField

class SparkSubmissionJobUploadStorageWithUploadPathPanel
    : Mvvm, Disposable, IdeaSettableControlWithRwLock<SparkSubmitJobUploadStorageModel>, ILogger {
    private val jobUploadStorageTitle = "Job Upload Storage"
    private val uploadPathLabel = JLabel("Upload Path")
    private val uploadPathField = JTextField().apply {
        isEditable = false
    }

    private val storagePanel = SparkSubmissionJobUploadStoragePanel().apply {
        Disposer.register(this@SparkSubmissionJobUploadStorageWithUploadPathPanel, this@apply)
    }

    private val hideableJobUploadStoragePanel = AccessibleHideableTitledPanel(jobUploadStorageTitle, storagePanel.view)

    override val view by lazy {
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

        formBuilder.buildPanel()
    }

    inner class ViewModel : Mvvm.ViewModel, DisposableObservers() {
        val uploadStorage
                get() = storagePanel.viewModel

        val clusterSelectedSubject
            get() = uploadStorage.clusterSelectedSubject

        private val ideaSchedulers = IdeaSchedulers()

        fun getCurrentUploadFieldText() : String? = uploadPathField.text?.trim()

        init {
            uploadStorage.validatedStorageUploadUri
                    .observeOn(ideaSchedulers.dispatchUIThread())
                    .subscribe { uploadPathField.text = it }
        }
    }

    override val viewModel = ViewModel().apply {
        Disposer.register(this@SparkSubmissionJobUploadStorageWithUploadPathPanel, this@apply)
    }

    override val model: SparkSubmitJobUploadStorageModel
        get() = SparkSubmitJobUploadStorageModel().apply { getData(this) }

    override fun readWithLock(to: SparkSubmitJobUploadStorageModel) {
        // Component -> Data
        to.uploadPath = uploadPathField.text
        to.storageAccountType = viewModel.uploadStorage.deployStorageTypesModel.selectedItem as? SparkSubmitStorageType

        storagePanel.readWithLock(to)
    }

    override fun writeWithLock(from: SparkSubmitJobUploadStorageModel) {
        viewModel.uploadStorage.apply {
            if (deployStorageTypeSelection != from.storageAccountType) {
                if (deployStorageTypesModel.size == 0) {
                    deployStorageTypesModel = ImmutableComboBoxModel(
                            from.storageAccountType?.let { arrayOf(it) } ?: emptyArray())
                }

                deployStorageTypeSelection = from.storageAccountType
            }
        }

        uploadPathField.text = from.uploadPath

        storagePanel.writeWithLock(from)
    }

    override fun dispose() {
    }
}