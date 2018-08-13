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

package com.microsoft.azure.hdinsight.spark.ui;

import com.google.common.collect.ImmutableList;
import com.intellij.openapi.fileChooser.FileChooser;
import com.intellij.openapi.fileChooser.FileChooserDescriptor;
import com.intellij.openapi.options.ConfigurationException;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.psi.PsiClass;
import com.intellij.packaging.artifacts.Artifact;
import com.intellij.packaging.impl.artifacts.ArtifactUtil;
import com.intellij.packaging.impl.elements.ManifestFileUtil;
import com.intellij.ui.ListCellRendererWrapper;
import com.microsoft.azure.hdinsight.common.ClusterManagerEx;
import com.microsoft.azure.hdinsight.common.mvc.SettableControl;
import com.microsoft.azure.hdinsight.sdk.cluster.HDInsightAdditionalClusterDetail;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.intellij.helpers.ManifestFileUtilsEx;
import com.microsoft.azure.hdinsight.spark.common.SparkSubmissionJobConfigCheckResult;
import com.microsoft.azure.hdinsight.spark.common.SparkSubmissionJobConfigCheckStatus;
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitModel;
import com.microsoft.azuretools.authmanage.AuthMethodManager;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import com.microsoft.tooling.msservices.components.DefaultLoader;
import org.apache.commons.lang3.StringUtils;

import javax.swing.*;
import java.awt.event.ItemEvent;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Spark Batch Application Submission UI control class
 */
public class SparkSubmissionContentPanelConfigurable implements SettableControl<SparkSubmitModel> {
    @NotNull
    private final Project myProject;

    private SparkSubmissionContentPanel submissionPanel;
    private JPanel myWholePanel;

    @NotNull
    private SparkSubmitModel submitModel;

    public SparkSubmissionContentPanelConfigurable(@NotNull SparkSubmitModel model,
                                                   @NotNull SparkSubmissionContentPanel submissionPanel) {
        this.submissionPanel = submissionPanel;
        this.submitModel = model;
        this.myProject = model.getProject();

        submissionPanel.getJobConfigurationTable().setModel(submitModel.getTableModel());
        this.submissionPanel.getClustersListComboBox().getComboBox().setModel(submitModel.getClusterComboBoxModel());
        this.submissionPanel.getClustersListComboBox().getComboBox().setRenderer(new ListCellRendererWrapper<IClusterDetail>() {
            @Override
            public void customize(JList jList, @Nullable IClusterDetail cluster, int i, boolean b, boolean b1) {
                this.setText(cluster == null ? null : cluster.getTitle());
            }
        });

        this.submissionPanel.getSelectedArtifactComboBox().setModel(submitModel.getArtifactComboBoxModel());
        this.submissionPanel.getSelectedArtifactComboBox().setRenderer(new ListCellRendererWrapper<Artifact>() {
            @Override
            public void customize(JList jList, @Nullable Artifact artifact, int i, boolean b, boolean b1) {
                this.setText(artifact == null ? null : artifact.getName());
            }
        });

        submissionPanel.getMainClassTextField().addActionListener(e -> {
                    PsiClass selected = submissionPanel.getLocalArtifactRadioButton().isSelected() ?
                            new ManifestFileUtilsEx(myProject).selectMainClass(
                                    new JarFileSystemImpl().findFileByPath(
                                            submissionPanel.getSelectedArtifactTextField().getText() + "!/")) :
                            ManifestFileUtil.selectMainClass(myProject, submissionPanel.getMainClassTextField().getText());
                    if (selected != null) {
                        submissionPanel.getMainClassTextField().setText(selected.getQualifiedName());
                    }
                }
        );

        this.submissionPanel.addClusterListRefreshActionListener(e -> refreshClusterListAsync());
        this.submissionPanel.getClustersListComboBox().getComboBox().addItemListener(e -> {
            switch (e.getStateChange()) {
            case ItemEvent.SELECTED:
                if (e.getItem() != null) {
                    IClusterDetail cluster = (IClusterDetail) e.getItem();
                    getSubmissionPanel().getClusterSelectedSubject().onNext(cluster.getName());
                }
                break;
            default:
            }
        });

        this.submissionPanel.addJobConfigurationLoadButtonActionListener(e -> {
            FileChooserDescriptor fileChooserDescriptor =
                    new FileChooserDescriptor(true, false, false, false, false, false);

            fileChooserDescriptor.setTitle("Select Spark Property File");

            Optional.ofNullable(FileChooser.chooseFile(fileChooserDescriptor, null, null))
                    .map(VirtualFile::getCanonicalPath)
                    .ifPresent(this::loadJobConfigMapFromPropertyFile);
        });

        this.submissionPanel.getIntelliJArtifactRadioButton().addActionListener(e ->
                refreshAndSelectArtifact(getSubmitModel().getArtifactComboBoxModel(),
                                         getSelectedArtifact() == null ? null : getSelectedArtifact().getName()));

        this.submissionPanel.updateTableColumn();

        refreshClusterListAsync();
    }

    @NotNull
    protected ImmutableList<IClusterDetail> getClusterDetails() {
        return ClusterManagerEx.getInstance().getClusterDetailsWithoutAsync(true);
    }

    protected void createUIComponents() {
        // Customized UI creation
    }

    @NotNull
    public SparkSubmitModel getSubmitModel() {
        return submitModel;
    }

    @NotNull
    public JComponent getComponent() {
        return submissionPanel;
    }

    protected void refreshClusterSelection(@NotNull List<IClusterDetail> clusters) {
        Optional<String> selectedClusterTitle = getSubmitModel().getSelectedClusterDetail()
                .map(IClusterDetail::getTitle);
        resetClusterDetailsToComboBoxModel(getSubmitModel(), clusters);
        if (selectedClusterTitle.isPresent()) {
            selectCluster(getSubmitModel(), selectedClusterTitle.get(), IClusterDetail::getTitle);
        } else {
            selectCluster(getSubmitModel(), getSubmitModel().getSubmissionParameter().getClusterName(), IClusterDetail::getName);
        }
    }

    protected void refreshClusterListAsync() {
        submissionPanel.setClustersListRefreshEnabled(false);

        DefaultLoader.getIdeHelper().executeOnPooledThread(() -> {
            List<IClusterDetail> cachedClusters = ClusterManagerEx.getInstance().getClusterDetailsWithoutAsync(true);
            refreshClusterSelection(cachedClusters);

            submissionPanel.setClustersListRefreshEnabled(true);
        });

    }

    private void refreshAndSelectArtifact(final @NotNull DefaultComboBoxModel<Artifact> artifactModel,
                                          final @Nullable String artifactName) {
        final List<Artifact> artifacts = ArtifactUtil.getArtifactWithOutputPaths(myProject);

        artifactModel.removeAllElements();

        for (int i = 0; i < artifacts.size(); i++) {
            if (StringUtils.equals(artifacts.get(i).getName(), artifactName)) {
                artifactModel.addElement(artifacts.get(i));         // Add with select it
            } else {
                artifactModel.insertElementAt(artifacts.get(i), i); // Insert without select it
            }
        }
    }

    protected void resetClusterDetailsToComboBoxModel(@NotNull SparkSubmitModel destSubmitModel, @NotNull List<IClusterDetail> cachedClusterDetails) {
        List<IClusterDetail> clusterDetails = new ArrayList<>();

        try {
            if (AuthMethodManager.getInstance().isSignedIn()) {
                clusterDetails = cachedClusterDetails;
            } else {
                clusterDetails = cachedClusterDetails.stream()
                        .filter(HDInsightAdditionalClusterDetail.class::isInstance)
                        .collect(Collectors.toList());
            }
        } catch (IOException ignored) { }

        destSubmitModel.getClusterComboBoxModel().removeAllElements();
        clusterDetails.forEach(clusterDetail -> destSubmitModel.getClusterComboBoxModel().addElement(clusterDetail));
    }

    private void loadJobConfigMapFromPropertyFile(String propertyFilePath) {
        getSubmitModel().getTableModel().loadJobConfigMapFromPropertyFile(propertyFilePath);
    }

    private void selectCluster(final SparkSubmitModel destSubmitModel,
                               final @Nullable String clusterProperty,
                               final Function<? super IClusterDetail, String> clusterPropertyMapper) {
        final DefaultComboBoxModel<IClusterDetail> clusterComboBoxModel = destSubmitModel.getClusterComboBoxModel();

        getClusterDetails().stream()
                .filter(Objects::nonNull)
                .filter(clusterDetail -> StringUtils.equals(clusterPropertyMapper.apply(clusterDetail), clusterProperty))
                .findFirst()
                .ifPresent(cluster-> {
                    if (clusterComboBoxModel.getIndexOf(cluster) >= 0) {
                        clusterComboBoxModel.setSelectedItem(cluster);
                    }
                });
    }

    public void setClusterSelectionEnabled(boolean enabled) {
        submissionPanel.getClustersListComboBox().setEnabled(enabled);
    }

    @Override
    public void setData(@NotNull SparkSubmitModel data) {
        // Data -> Component

        resetClusterDetailsToComboBoxModel(getSubmitModel(), getClusterDetails());
        if (data.getSelectedClusterDetail().isPresent()) {
            selectCluster(getSubmitModel(), data.getSelectedClusterDetail().get().getTitle(), IClusterDetail::getTitle);
        } else {
            selectCluster(getSubmitModel(), data.getClusterName(), IClusterDetail::getName);
        }

        if (data.getIsLocalArtifact()) {
            submissionPanel.getLocalArtifactRadioButton().setSelected(true);
        }

        submissionPanel.getSelectedArtifactTextField().setText(data.getLocalArtifactPath());
        submissionPanel.getMainClassTextField().setText(data.getMainClassName());
        submissionPanel.getCommandLineTextField().setText(String.join(" ", data.getCommandLineArgs()));
        submissionPanel.getReferencedJarsTextField().setText(String.join(";", data.getReferenceJars()));
        submissionPanel.getReferencedFilesTextField().setText(String.join(";", data.getReferenceFiles()));

        // update job configuration table
        getSubmitModel().getTableModel().loadJobConfigMap(data.getTableModel().getJobConfigMap());
        refreshAndSelectArtifact(getSubmitModel().getArtifactComboBoxModel(), data.getArtifactName());
    }

    @Override
    public void getData(@NotNull SparkSubmitModel data) {
        // Component -> Data

        String selectedArtifactName = Optional.ofNullable(getSelectedArtifact())
                .map(Artifact::getName)
                .orElse("");

        String className = submissionPanel.getMainClassTextField().getText().trim();

        String localArtifactPath = submissionPanel.getSelectedArtifactTextField().getText();

        String selectedClusterName = getSubmitModel().getSelectedClusterDetail()
                .map(IClusterDetail::getName)
                .orElse("");

        List<String> referencedFileList = Arrays.stream(submissionPanel.getReferencedFilesTextField().getText().split(";"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        List<String> uploadedFilePathList = Arrays.stream(submissionPanel.getReferencedJarsTextField().getText().split(";"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        List<String> argsList = Arrays.stream(submissionPanel.getCommandLineTextField().getText().split(" "))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        boolean isLocalArtifact = submissionPanel.getLocalArtifactRadioButton().isSelected();

        // submission parameters
        data.setClusterName(selectedClusterName);
        data.setIsLocalArtifact(isLocalArtifact);
        data.setArtifactName(selectedArtifactName);
        data.setLocalArtifactPath(localArtifactPath);
        data.setFilePath(null);
        data.setMainClassName(className);
        data.setReferenceFiles(referencedFileList);
        data.setReferenceJars(uploadedFilePathList);
        data.setCommandLineArgs(argsList);

        // Sub models
        resetClusterDetailsToComboBoxModel(data, getClusterDetails());
        getSubmitModel().getSelectedClusterDetail()
                   .map(IClusterDetail::getTitle)
                   .ifPresent(selectedTitle -> selectCluster(data, selectedTitle, IClusterDetail::getTitle));

        data.getTableModel().loadJobConfigMap(getSubmitModel().getTableModel().getJobConfigMap());

        // Artifact selection
        refreshAndSelectArtifact(data.getArtifactComboBoxModel(), selectedArtifactName);
    }

    public void validate() throws ConfigurationException {
        // FIXME!!! Need to re-design submission panel

        SparkSubmissionJobConfigCheckResult confTableCheckResult = getSubmitModel().getTableModel().getFirstCheckResults();
        if (confTableCheckResult.getStatus() == SparkSubmissionJobConfigCheckStatus.Error) {
            throw new ConfigurationException("Job Configuration " + confTableCheckResult.getMessaqge());
        }

    }

    public SparkSubmissionContentPanel getSubmissionPanel() {
        return submissionPanel;
    }

    @Nullable
    private Artifact getSelectedArtifact() {
        return (Artifact) submissionPanel.getSelectedArtifactComboBox().getSelectedItem();
    }
}
