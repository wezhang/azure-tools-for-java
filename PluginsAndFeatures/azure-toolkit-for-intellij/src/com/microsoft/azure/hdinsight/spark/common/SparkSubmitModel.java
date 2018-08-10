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

import com.intellij.openapi.command.impl.DummyProject;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.InvalidDataException;
import com.intellij.packaging.artifacts.Artifact;
import com.intellij.packaging.impl.artifacts.ArtifactUtil;
import com.intellij.util.xmlb.XmlSerializer;
import com.intellij.util.xmlb.annotations.AbstractCollection;
import com.intellij.util.xmlb.annotations.Attribute;
import com.intellij.util.xmlb.annotations.Property;
import com.intellij.util.xmlb.annotations.Tag;
import com.intellij.util.xmlb.annotations.Transient;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import com.microsoft.azuretools.utils.Pair;
import org.jdom.Element;

import javax.swing.*;
import javax.swing.event.TableModelEvent;
import java.util.*;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

@Tag(SparkSubmitModel.SUBMISSION_CONTENT_NAME)
public class SparkSubmitModel {
    private static final String[] columns = {"Key", "Value", ""};

    static final String SUBMISSION_CONTENT_NAME = "spark_submission";
    private static final String SUBMISSION_CONTENT_JOB_CONF = "job_conf";

    @Transient
    @NotNull
    private Project project = DummyProject.getInstance();

    @Transient
    private Map<String, Artifact> artifactHashMap = new HashMap<>();

    @Transient
    @NotNull
    private SparkSubmissionParameter submissionParameter; // The parameters to packing submission related various

    @NotNull
    @Property(surroundWithTag = false)
    private SparkSubmitAdvancedConfigModel advancedConfigModel = new SparkSubmitAdvancedConfigModel();

    @Transient
    private DefaultComboBoxModel<IClusterDetail> clusterComboBoxModel;

    @Transient
    private DefaultComboBoxModel<String> artifactComboBoxModel;

    @Transient
    private SubmissionTableModel tableModel = new SubmissionTableModel(columns);

    public SparkSubmitModel() {
        this.submissionParameter = new SparkSubmissionParameter();
    }

    public SparkSubmitModel(@NotNull Project project) {
        this(project, new SparkSubmissionParameter());
    }

    public SparkSubmitModel(@NotNull Project project, @NotNull SparkSubmissionParameter submissionParameter) {
        this.project = project;
        this.clusterComboBoxModel = new DefaultComboBoxModel<>();
        this.artifactComboBoxModel = new DefaultComboBoxModel<>();
        this.advancedConfigModel = new SparkSubmitAdvancedConfigModel();
        this.submissionParameter = submissionParameter;

        final List<Artifact> artifacts = ArtifactUtil.getArtifactWithOutputPaths(project);

        for (Artifact artifact : artifacts) {
            artifactHashMap.put(artifact.getName(), artifact);
            artifactComboBoxModel.addElement(artifact.getName());
            if (artifactComboBoxModel.getSize() == 0) {
                artifactComboBoxModel.setSelectedItem(artifact.getName());
            }
        }

        int index = artifactComboBoxModel.getIndexOf(submissionParameter.getArtifactName());
        if (index != -1) {
            artifactComboBoxModel.setSelectedItem(submissionParameter.getArtifactName());
        }

        initializeTableModel(tableModel);
    }

    @Transient
    @NotNull
    public SparkSubmissionParameter getSubmissionParameter() {
        // Apply from table model
        submissionParameter.applyFlattedJobConf(tableModel.getJobConfigMap());

        return submissionParameter;
    }

    @NotNull
    public SparkSubmitAdvancedConfigModel getAdvancedConfigModel() { return advancedConfigModel; }

    @Transient
    public Optional<IClusterDetail> getSelectedClusterDetail() {
        return Optional.ofNullable(getClusterComboBoxModel().getSelectedItem())
                .map(IClusterDetail.class::cast);
    }

    @Transient
    @NotNull
    public DefaultComboBoxModel<IClusterDetail> getClusterComboBoxModel() {
        return clusterComboBoxModel;
    }

    @Transient
    public DefaultComboBoxModel<String> getArtifactComboBoxModel() {
        return artifactComboBoxModel;
    }

    @Attribute("cluster_name")
    public String getClusterName() {
        return getSubmissionParameter().getClusterName();
    }

    @Attribute("cluster_name")
    public void setClusterName(String clusterName) {
        getSubmissionParameter().setClusterName(clusterName);
    }

    @Attribute("is_local_artifact")
    public boolean getIsLocalArtifact() {
        return getSubmissionParameter().isLocalArtifact();
    }

    @Attribute("is_local_artifact")
    public void setIsLocalArtifact(boolean isLocalArtifact) {
        getSubmissionParameter().setLocalArtifact(isLocalArtifact);
    }

    @Attribute("artifact_name")
    public String getArtifactName() {
        return getSubmissionParameter().getArtifactName();
    }

    @Attribute("artifact_name")
    public void setArtifactName(String artifactName) {
        getSubmissionParameter().setArtifactName(artifactName);
    }

    @Attribute("local_artifact_path")
    public String getLocalArtifactPath() {
        return getSubmissionParameter().getLocalArtifactPath();
    }

    @Attribute("local_artifact_path")
    public void setLocalArtifactPath(String localArtifactPath) {
        getSubmissionParameter().setLocalArtifactPath(localArtifactPath);
    }
    @Attribute("file_path")
    public String getFilePath() {
        return getSubmissionParameter().getFile();
    }

    @Attribute("file_path")
    public void setFilePath(String filePath) {
        getSubmissionParameter().setFilePath(filePath);
    }

    @Attribute("classname")
    public String getMainClassName() {
        return getSubmissionParameter().getMainClassName();
    }

    @Attribute("classname")
    public void setMainClassName(String mainClassName) {
        getSubmissionParameter().setClassName(mainClassName);
    }

    @Tag("cmd_line_args")
    @AbstractCollection(surroundWithTag = false)
    public List<String> getCommandLineArgs() {
        return getSubmissionParameter().getArgs();
    }

    @Tag("cmd_line_args")
    @AbstractCollection(surroundWithTag = false)
    public void setCommandLineArgs(List<String> cmdArgs) {
        getSubmissionParameter().getArgs().clear();
        getSubmissionParameter().getArgs().addAll(cmdArgs);
    }

    @Tag("ref_jars")
    @AbstractCollection(surroundWithTag = false)
    public List<String> getReferenceJars() {
        return getSubmissionParameter().getReferencedJars();
    }

    @Tag("ref_jars")
    @AbstractCollection(surroundWithTag = false)
    public void setReferenceJars(List<String> refJars) {
        getSubmissionParameter().getReferencedJars().clear();
        getSubmissionParameter().getReferencedJars().addAll(refJars);
    }

    @Tag("ref_files")
    @AbstractCollection(surroundWithTag = false)
    public List<String> getReferenceFiles() {
        return getSubmissionParameter().getReferencedFiles();
    }

    @Tag("ref_files")
    @AbstractCollection(surroundWithTag = false)
    public void setReferenceFiles(List<String> refFiles) {
        getSubmissionParameter().getReferencedFiles().clear();
        getSubmissionParameter().getReferencedFiles().addAll(refFiles);
    }

    @Transient
    @Nullable
    public Artifact getArtifact() {
        return Optional.of(getSubmissionParameter())
                .map(SparkSubmissionParameter::getArtifactName)
                .filter(name -> artifactHashMap.containsKey(name))
                .map(artifactHashMap::get)
                .orElse(null);
    }

    @Transient
    @NotNull
    public Project getProject() {
        return project;
    }

    @Transient
    public SubmissionTableModel getTableModel() {
        return tableModel;
    }

    @Transient
    public Optional<String> getArtifactPath() {
        String buildJarPath = getSubmissionParameter().isLocalArtifact() ?
                getSubmissionParameter().getLocalArtifactPath() :
                ((artifactHashMap.get(getSubmissionParameter().getArtifactName()).getOutputFilePath()));

        return Optional.ofNullable(buildJarPath);
    }

    @NotNull
    protected Pair<String, ? extends Object>[] getDefaultParameters() {
        return Arrays.stream(SparkSubmissionParameter.defaultParameters)
                .map(p -> new Pair<>(p.first(), p.second()))
                .toArray((IntFunction<Pair<String, Object>[]>) Pair[]::new);
    }

    private void initializeTableModel(final SubmissionTableModel tableModel) {
        if (submissionParameter.getJobConfig().isEmpty()) {
            for (int i = 0; i < getDefaultParameters().length; ++i) {
                tableModel.addRow(getDefaultParameters()[i].first(), "");
            }

            if (!tableModel.hasEmptyRow()) {
                tableModel.addEmptyRow();
            }
        } else {
            tableModel.loadJobConfigMap(submissionParameter.flatJobConfig());
        }

        tableModel.addTableModelListener(e -> {
            if (e.getType() == TableModelEvent.UPDATE && (e.getLastRow() + 1) == tableModel.getRowCount()) {
                tableModel.addEmptyRow();
            }
        });
    }

    public Element exportToElement() {
        Element submitModelElement = XmlSerializer.serialize(this);

        // To keep back-compatible of XML serialization
        submitModelElement.addContent(new Element(SUBMISSION_CONTENT_JOB_CONF)
                .setAttributes(this.tableModel.getJobConfigMap().entrySet().stream()
                        .filter(entry -> entry.getKey() != null && !entry.getKey().trim().isEmpty())
                        .map(entry -> new org.jdom.Attribute(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList())));

        return submitModelElement;
    }

    public SparkSubmitModel applyFromElement(@NotNull Element rootElement) throws InvalidDataException{
        XmlSerializer.deserializeInto(this, rootElement);

        // To keep back-compatible of XML serialization
        Element jobConfElem = rootElement.getChild(SUBMISSION_CONTENT_JOB_CONF);
        if (jobConfElem != null) {
            Map<String, String> jobConf = jobConfElem.getAttributes().stream()
                    .collect(Collectors.toMap(org.jdom.Attribute::getName, org.jdom.Attribute::getValue));

            getSubmissionParameter().applyFlattedJobConf(jobConf);
            this.tableModel.loadJobConfigMap(getSubmissionParameter().flatJobConfig());
        }

        return this;
    }
}
