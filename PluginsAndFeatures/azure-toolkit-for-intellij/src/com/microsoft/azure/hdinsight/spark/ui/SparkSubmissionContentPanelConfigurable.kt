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

package com.microsoft.azure.hdinsight.spark.ui

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSortedSet
import com.intellij.execution.configurations.RuntimeConfigurationError
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.application.ModalityState
import com.intellij.openapi.options.ConfigurationException
import com.intellij.openapi.project.Project
import com.intellij.openapi.vfs.impl.jar.JarFileSystemImpl
import com.intellij.packaging.artifacts.Artifact
import com.intellij.packaging.impl.artifacts.ArtifactUtil
import com.intellij.packaging.impl.elements.ManifestFileUtil
import com.microsoft.azure.hdinsight.common.ClusterManagerEx
import com.microsoft.azure.hdinsight.common.logger.ILogger
import com.microsoft.azure.hdinsight.common.mvc.SettableControl
import com.microsoft.azure.hdinsight.metadata.ClusterMetaDataService
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitModel
import com.microsoft.azure.hdinsight.spark.common.SubmissionTableModel
import com.microsoft.intellij.helpers.ManifestFileUtilsEx
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.tuple.Pair
import rx.Observable
import rx.schedulers.Schedulers
import rx.subjects.BehaviorSubject
import java.awt.event.ActionListener

import javax.swing.*
import java.awt.event.ItemEvent
import java.util.Arrays
import java.util.function.Function
import java.util.stream.Collectors

/**
 * Spark Batch Application Submission UI control class
 */
open class SparkSubmissionContentPanelConfigurable(private val myProject: Project) : SettableControl<SparkSubmitModel>, ILogger {

    protected var submissionPanel = SparkSubmissionContentPanel()
    private val jobUploadStorageCtrl: SparkSubmissionJobUploadStorageCtrl

    // Cluster refresh publish subject with preselected cluster name as event
    private var clustersRefreshSub: BehaviorSubject<String>? = null

    val storageWithUploadPathPanel: SparkSubmissionJobUploadStorageWithUploadPathPanel
        get() = submissionPanel.storageWithUploadPathPanel

    protected open val clusterDetails: ImmutableSortedSet<out IClusterDetail>
        get() = ImmutableSortedSet.copyOf({ x, y -> x.title.compareTo(y.title, ignoreCase = true) },
                ClusterMetaDataService.getInstance().cachedClusterDetails)

    protected open fun getClusterDetailsWithRefresh(): Observable<ImmutableSortedSet<out IClusterDetail>> =
        Observable.fromCallable<ImmutableList<IClusterDetail>> { ClusterManagerEx.getInstance().clusterDetails }
                .map { list ->
                    ImmutableSortedSet.copyOf<IClusterDetail>({ x, y -> x.title.compareTo(y.title, ignoreCase = true) }, list)
                }

    protected open val type: String
        get() = "HDInsight"

    open val component: JComponent
        get() = submissionPanel

    open val selectedClusterDetail: IClusterDetail?
        get() = submissionPanel.clustersModel.selectedItem as IClusterDetail

    private val selectedArtifact: Artifact?
        get() = submissionPanel.selectedArtifactComboBox.selectedItem as Artifact

    init {

        registerCtrlListeners()

        this.jobUploadStorageCtrl = object : SparkSubmissionJobUploadStorageCtrl(storageWithUploadPathPanel) {
            override fun getClusterName(): String? = selectedClusterDetail?.name

            override fun getClusterDetail(): IClusterDetail? {
                return clusterDetails.stream()
                        .filter { clusterDetail -> clusterDetail.name == getClusterName() }
                        .findFirst()
                        .orElse(null)
            }
        }
        this.clustersRefreshSub = BehaviorSubject.create()
    }

    private fun registerCtrlListeners() {
        this.submissionPanel.mainClassTextField.addActionListener { _ ->
            val selected = if (submissionPanel.localArtifactPrompt.isSelected)
                ManifestFileUtilsEx(myProject).selectMainClass(
                        JarFileSystemImpl().findFileByPath("${submissionPanel.localArtifactTextField.text}!/"))
            else
                ManifestFileUtil.selectMainClass(myProject, submissionPanel.mainClassTextField.text)

            if (selected != null) {
                submissionPanel.mainClassTextField.setText(selected.qualifiedName)
            }
        }

        this.submissionPanel.addClusterListRefreshActionListener(ActionListener {
            refreshClusterListAsync(selectedClusterDetail?.name)
        })

        this.submissionPanel.clustersListComboBox.comboBox.addItemListener { e ->
            when (e.stateChange) {
                ItemEvent.SELECTED -> if (e.item != null) {
                    val cluster = e.item as IClusterDetail
                    onClusterSelected(cluster)
                }
            }
        }

        this.submissionPanel.ideaArtifactPrompt.addActionListener { refreshAndSelectArtifact(selectedArtifact?.name) }

        this.submissionPanel.clustersModel = ImmutableComboBoxModel(clusterDetails.toTypedArray())

        this.submissionPanel.addPropertyChangeListener("ancestor") { event ->
            if (event.newValue != null) {
                // Being added
                if (clustersRefreshSub == null) {
                    clustersRefreshSub = BehaviorSubject.create()
                }

                clustersRefreshSub!!
                        .doOnNext { setClusterRefreshEnabled(false) }
                        .flatMap { preSelectedClusterName ->
                            getClusterDetailsWithRefresh()
                                    .subscribeOn(Schedulers.io())
                                    .map { clusters -> Pair.of(preSelectedClusterName, clusters) }
                                    .onErrorReturn { err ->
                                        log().warn(String.format("Project %s failed to refresh %s: %s",
                                                myProject.name, type, err))

                                        Pair.of(preSelectedClusterName, ImmutableSortedSet.of())
                                    }
                        }
                        .doOnEach { setClusterRefreshEnabled(true) }
                        .subscribe(
                                { selectedClustersPair ->
                                    submissionPanel.clustersModel = ImmutableComboBoxModel(
                                            selectedClustersPair.right.toTypedArray())

                                    if (selectedClustersPair.left != null) {
                                        selectCluster(selectedClustersPair.left, Function { it.name })
                                    }
                                },
                                { err ->
                                    log().error(String.format("Project %s failed to process subject %s: %s",
                                            myProject.name, type, err))
                                })
            } else if (clustersRefreshSub != null) {
                // Being removed
                clustersRefreshSub!!.onCompleted()

                clustersRefreshSub = null
            }
        }
    }

    protected open fun onClusterSelected(cluster: IClusterDetail) {
        submissionPanel.clusterSelectedSubject.onNext(cluster.name)
        jobUploadStorageCtrl.selectCluster(cluster.name)
    }

    @Synchronized
    private fun refreshClusterListAsync(preSelectedClusterName: String?) {
        clustersRefreshSub?.onNext(preSelectedClusterName)
    }

    @Synchronized
    private fun refreshAndSelectArtifact(artifactName: String?) {
        val artifactModel = submissionPanel.selectedArtifactComboBox.model as DefaultComboBoxModel<Artifact>

        val artifacts = ArtifactUtil.getArtifactWithOutputPaths(myProject)

        artifactModel.removeAllElements()

        for (i in artifacts.indices) {
            if (StringUtils.equals(artifacts[i].name, artifactName)) {
                artifactModel.addElement(artifacts[i])         // Add with select it
            } else {
                artifactModel.insertElementAt(artifacts[i], i) // Insert without select it
            }
        }

        // If no element selected, select the first one as default
        if (StringUtils.isBlank(artifactName) && artifactModel.selectedItem == null && artifactModel.size > 0) {
            artifactModel.selectedItem = artifactModel.getElementAt(0)
        }
    }

    // Select cluster from the cluster combo box model
    // Returns true for find and select the cluster
    private fun selectCluster(clusterProperty: String?,
                              clusterPropertyMapper: Function<in IClusterDetail, String>): Boolean {
        val clustersModel = submissionPanel.clustersModel

        for (i in 0 until clustersModel.size) {
            if (StringUtils.equals(clusterProperty, clusterPropertyMapper.apply(clustersModel.getElementAt(i)))) {
                clustersModel.selectedItem = clustersModel.getElementAt(i)
                break
            }
        }

        return if (clustersModel.selectedItem != null) {
            onClusterSelected(clustersModel.selectedItem as IClusterDetail)

            true
        } else {
            false
        }
    }

    private fun setClusterRefreshEnabled(enabled: Boolean) {
        ApplicationManager.getApplication().invokeAndWait({
            submissionPanel.setClustersListRefreshEnabled(enabled)
        }, ModalityState.any())
    }

    fun setClusterSelectionEnabled(enabled: Boolean) {
        ApplicationManager.getApplication().invokeAndWait({
            submissionPanel.clustersListComboBox.isEnabled = enabled
        }, ModalityState.any())
    }

    override fun setData(data: SparkSubmitModel) {
        // Data -> Component

        // The clusters combo box model and artifacts model are project context related,
        // so just refresh them if needed, rather than reading from the data

        // Scenarios
        // 1. Cluster refresh in progress, the list model have choice, select cluster by cluster name
        // 2. Cluster refresh in progress, the list model is empty, save cluster name in submit model
        // 3. Cluster list got, but no selection before, select cluster by cluster name
        ApplicationManager.getApplication().invokeAndWait({
            if (data.clusterComboBoxModel != null) {
                submissionPanel.clustersModel = data.clusterComboBoxModel
            }
            submissionPanel.selectedArtifactComboBox.model = data.artifactComboBoxModel

            if (!selectCluster(data.clusterName, Function { it.name })) {
                refreshClusterListAsync(data.clusterName)
            }

            if (data.isLocalArtifact) {
                submissionPanel.localArtifactPrompt.isSelected = true
            }

            submissionPanel.localArtifactTextField.text = data.localArtifactPath
            submissionPanel.mainClassTextField.text = data.mainClassName
            submissionPanel.commandLineTextField.text = data.commandLineArgs.joinToString(" ")
            submissionPanel.referencedJarsTextField.text = data.referenceJars.joinToString(";")
            submissionPanel.referencedFilesTextField.text = data.referenceFiles.joinToString(";")

            // update job configuration table
            submissionPanel.jobConfigurationTable.model = data.tableModel

            refreshAndSelectArtifact(data.artifactName)
        }, ModalityState.any())

        // set Job Upload Storage panel data
        storageWithUploadPathPanel.setData(data.jobUploadStorageModel)
    }

    override fun getData(data: SparkSubmitModel) {
        // Component -> Data

        val selectedArtifactName = selectedArtifact?.name ?: ""
        val className = submissionPanel.mainClassTextField.text.trim()
        val selectedClusterName = selectedClusterDetail?.name ?: ""

        val referencedFileList = Arrays.stream(submissionPanel.referencedFilesTextField.text.split(";").dropLastWhile { it.isEmpty() }.toTypedArray())
                .map { it.trim() }
                .filter { s -> !s.isEmpty() }
                .collect(Collectors.toList())

        val uploadedFilePathList = Arrays.stream(submissionPanel.referencedJarsTextField.text.split(";").dropLastWhile { it.isEmpty() }.toTypedArray())
                .map { it.trim() }
                .filter { s -> !s.isEmpty() }
                .collect(Collectors.toList())

        val argsList = Arrays.stream(submissionPanel.commandLineTextField.text.split(" ").dropLastWhile { it.isEmpty() }.toTypedArray())
                .map { it.trim() }
                .filter { s -> !s.isEmpty() }
                .collect(Collectors.toList())

        data.apply {
            // submission parameters
            clusterName = selectedClusterName
            isLocalArtifact = submissionPanel.localArtifactPrompt.isSelected
            artifactName = selectedArtifactName
            localArtifactPath = submissionPanel.localArtifactTextField.text
            filePath = null
            mainClassName = className
            referenceFiles = referencedFileList
            referenceJars = uploadedFilePathList
            commandLineArgs = argsList

            tableModel = submissionPanel.jobConfigurationTable.model as SubmissionTableModel
            clusterComboBoxModel = submissionPanel.clustersModel
        }

        // get Job upload storage panel data
        storageWithUploadPathPanel.getData(data.jobUploadStorageModel)
    }

    @Throws(ConfigurationException::class)
    open fun validate() {
        submissionPanel.checkInputs()

        if (!jobUploadStorageCtrl.isCheckPassed) {
            throw RuntimeConfigurationError("Can't save the configuration since " + jobUploadStorageCtrl.resultMessage?.toLowerCase())
        }
    }
}
