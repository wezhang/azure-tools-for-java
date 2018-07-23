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

import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.InvalidDataException;
import com.intellij.packaging.artifacts.Artifact;
import com.intellij.packaging.impl.artifacts.ArtifactUtil;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.spark.uihelper.InteractiveTableModel;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import com.microsoft.azuretools.azurecommons.helpers.StringHelper;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.Text;

import javax.swing.*;
import javax.swing.event.TableModelEvent;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.microsoft.azure.hdinsight.spark.common.SparkSubmitAdvancedConfigModel.SUBMISSION_CONTENT_SSH_CERT;

public class SparkSubmitModel {

    private static final String[] columns = {"Key", "Value", ""};

    protected static final String SUBMISSION_CONTENT_NAME = "spark_submission";
    private static final String SUBMISSION_ATTRIBUTE_CLUSTER_NAME = "cluster_name";
    private static final String SUBMISSION_ATTRIBUTE_SELECTED_CLUSTER = "selected_cluster";
    private static final String SUBMISSION_ATTRIBUTE_IS_LOCAL_ARTIFACT = "is_local_artifact";
    private static final String SUBMISSION_ATTRIBUTE_ARTIFACT_NAME = "artifact_name";
    private static final String SUBMISSION_ATTRIBUTE_LOCAL_ARTIFACT_PATH = "local_artifact_path";
    private static final String SUBMISSION_ATTRIBUTE_FILE_PATH = "file_path";
    private static final String SUBMISSION_ATTRIBUTE_CLASSNAME = "classname";
    private static final String SUBMISSION_CONTENT_JOB_CONF = "job_conf";
    private static final String SUBMISSION_CONTENT_COMMAND_LINE_ARGS = "cmd_line_args";
    private static final String SUBMISSION_CONTENT_REF_JARS = "ref_jars";
    private static final String SUBMISSION_CONTENT_REF_FILES = "ref_files";

    private static Map<Project, SparkSubmissionParameter> submissionParameterMap = new HashMap<>();

    @NotNull
    private final Project project;

    private List<IClusterDetail> cachedClusterDetails;
    private Map<String, IClusterDetail> mapClusterNameToClusterDetail = new HashMap<>();
    private Map<String, Artifact> artifactHashMap = new HashMap<>();

    // The parameters of UI settings
    @NotNull
    private SparkSubmissionParameter submissionParameter;

    @NotNull
    private SparkSubmitAdvancedConfigModel advancedConfigModel;

    private DefaultComboBoxModel<String> clusterComboBoxModel;

    private DefaultComboBoxModel<String> artifactComboBoxModel;

    private SubmissionTableModel tableModel = new SubmissionTableModel(columns);

    public SparkSubmitModel(@NotNull Project project) {
        this(project,
             Optional.ofNullable(submissionParameterMap.get(project))
                     .orElseGet(() -> new SparkSubmissionParameter(
                             "",
                             false,
                             "",
                             "",
                             "",
                             "",
                             new ArrayList<>(),
                             new ArrayList<>(),
                             new ArrayList<>(),
                             new HashMap<>())));
    }

    public SparkSubmitModel(@NotNull Project project, @NotNull SparkSubmissionParameter submissionParameter) {
        this.cachedClusterDetails = new ArrayList<>();
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

    @NotNull
    public SparkSubmissionParameter getSubmissionParameter() {
        return submissionParameter;
    }

    public void setSubmissionParameters(@NotNull SparkSubmissionParameter submissionParameters){
        this.submissionParameter = submissionParameters;
        submissionParameterMap.put(project, submissionParameter);
    }

    @NotNull
    public SparkSubmitAdvancedConfigModel getAdvancedConfigModel() { return advancedConfigModel; }

    public void setAdvancedConfigModel(@NotNull SparkSubmitAdvancedConfigModel advancedConfigModel) {
        this.advancedConfigModel = advancedConfigModel;
    }

    public Optional<IClusterDetail> getSelectedClusterDetail() {
        return Optional.ofNullable(mapClusterNameToClusterDetail.get(clusterComboBoxModel.getSelectedItem()));
    }

    public void setCachedClusterDetailsWithTitleMapping(List<IClusterDetail> cachedClusterDetails) {
        this.cachedClusterDetails = cachedClusterDetails;

        mapClusterNameToClusterDetail.clear();
        cachedClusterDetails.forEach(clusterDetail -> mapClusterNameToClusterDetail.put(clusterDetail.getTitle(), clusterDetail));
    }

    @NotNull
    public List<IClusterDetail> getCachedClusterDetails() {
        return cachedClusterDetails;
    }

    @NotNull
    public DefaultComboBoxModel<String> getClusterComboBoxModel() {
        return clusterComboBoxModel;
    }

    public DefaultComboBoxModel<String> getArtifactComboBoxModel() {
        return artifactComboBoxModel;
    }

    public boolean isLocalArtifact() {
        return submissionParameter.isLocalArtifact();
    }

    @Nullable
    public Artifact getArtifact() {
        return Optional.of(getSubmissionParameter())
                .map(SparkSubmissionParameter::getArtifactName)
                .filter(name -> artifactHashMap.containsKey(name))
                .map(artifactHashMap::get)
                .orElse(null);
    }

    @NotNull
    public Project getProject() {
        return project;
    }

    public SubmissionTableModel getTableModel() {
        return tableModel;
    }

    public Optional<String> getArtifactPath() {
        String buildJarPath = submissionParameter.isLocalArtifact() ?
                submissionParameter.getLocalArtifactPath() :
                ((artifactHashMap.get(submissionParameter.getArtifactName()).getOutputFilePath()));

        return Optional.ofNullable(buildJarPath);
    }

    public Map<String, Object> getJobConfigMap() {
        return tableModel.getJobConfigMap();
    }

    @NotNull
    protected Pair<String, String>[] getDefaultParameters() {
        return SparkSubmissionParameter.defaultParameters;
    }

    private void initializeTableModel(final InteractiveTableModel tableModel) {
        if (submissionParameter.getJobConfig().isEmpty()) {
            for (int i = 0; i < getDefaultParameters().length; ++i) {
                tableModel.addRow(getDefaultParameters()[i].first(), "");
            }
        } else {
            Map<String, Object> configs = submissionParameter.getJobConfig();

            Arrays.stream(SparkSubmissionParameter.parameterList).forEach(key -> {
                // Put the default empty value into the submission parameters job configuration
                if (!configs.containsKey(key)) {
                    configs.put(key, "");
                }

                tableModel.addRow(key, configs.get(key));
            });

            for (Map.Entry<String, Object> jobConfigEntry : configs.entrySet()) {
                String jobConfigKey = jobConfigEntry.getKey();
                Object jobConfigValue = jobConfigEntry.getValue();

                if (!StringHelper.isNullOrWhiteSpace(jobConfigKey) && !SparkSubmissionParameter.isSubmissionParameter(jobConfigKey)) {
                    if (jobConfigKey.equals(SparkSubmissionParameter.Conf)) {
                        SparkConfigures sparkConfigs;

                        if (jobConfigValue instanceof Map && !(sparkConfigs = new SparkConfigures(jobConfigValue)).isEmpty()) {
                            for (Map.Entry<String, Object> sparkConfigEntry : sparkConfigs.entrySet()) {
                                if (!StringHelper.isNullOrWhiteSpace(sparkConfigEntry.getKey())) {
                                    tableModel.addRow(sparkConfigEntry.getKey(), sparkConfigEntry.getValue());
                                }
                            }
                        }
                    } else {
                        tableModel.addRow(jobConfigKey, jobConfigValue);
                    }
                }
            }
        }

        if (!tableModel.hasEmptyRow()) {
            tableModel.addEmptyRow();
        }

        tableModel.addTableModelListener(e -> {
            if (e.getType() == TableModelEvent.UPDATE && (e.getLastRow() + 1) == tableModel.getRowCount()) {
                tableModel.addEmptyRow();
            }
        });
    }

    public Element exportToElement() {
        Element submitModelElement = new Element(SUBMISSION_CONTENT_NAME);

        SparkSubmissionParameter submissionParameter = getSubmissionParameter();

        submitModelElement.setAttribute(
                SUBMISSION_ATTRIBUTE_CLUSTER_NAME, submissionParameter.getClusterName());
        submitModelElement.setAttribute(
                SUBMISSION_ATTRIBUTE_IS_LOCAL_ARTIFACT, Boolean.toString(isLocalArtifact()));
        submitModelElement.setAttribute(
                SUBMISSION_ATTRIBUTE_ARTIFACT_NAME, submissionParameter.getArtifactName());
        submitModelElement.setAttribute(
                SUBMISSION_ATTRIBUTE_CLASSNAME, submissionParameter.getMainClassName());

        Map<String, Object> jobConf = submissionParameter.getJobConfig();
        Element jobConfElement = new Element(SUBMISSION_CONTENT_JOB_CONF);

        jobConfElement.setAttributes(Stream.concat(
                        jobConf.entrySet().stream()
                                .filter(entry -> SparkSubmissionParameter.isSubmissionParameter(entry.getKey())),
                        // The Spark Job Configuration needs to be separated
                        jobConf.entrySet().stream()
                                .filter(entry -> !SparkSubmissionParameter.isSubmissionParameter(entry.getKey()))
                                .filter(entry -> entry.getKey().equals(SparkSubmissionParameter.Conf))
                                .flatMap(entry -> new SparkConfigures(entry.getValue()).entrySet().stream())
                )
                .filter(entry -> !entry.getKey().trim().isEmpty())
                .map(entry -> new Attribute(entry.getKey(), entry.getValue().toString()))
                .collect(Collectors.toList()));

        submitModelElement.addContent(jobConfElement);

        Element cmdLineArgsElement = new Element(SUBMISSION_CONTENT_COMMAND_LINE_ARGS);
        cmdLineArgsElement.addContent(
                submissionParameter.getArgs().stream()
                        .filter(e -> !e.trim().isEmpty())
                        .map(Text::new).collect(Collectors.toList()));
        submitModelElement.addContent(cmdLineArgsElement);

        Element refJarsElement = new Element(SUBMISSION_CONTENT_REF_JARS);
        refJarsElement.addContent(
                submissionParameter.getReferencedJars().stream()
                        .filter(e -> !e.trim().isEmpty())
                        .map(Text::new).collect(Collectors.toList()));
        submitModelElement.addContent(refJarsElement);

        Element refFilesElement = new Element(SUBMISSION_CONTENT_REF_FILES);
        refFilesElement.addContent(
                submissionParameter.getReferencedFiles().stream()
                        .filter(e -> !e.trim().isEmpty())
                        .map(Text::new).collect(Collectors.toList()));
        submitModelElement.addContent(refFilesElement);

        SparkSubmitAdvancedConfigModel advConfModel = getAdvancedConfigModel();
        if (advConfModel.enableRemoteDebug) {
            advConfModel.setClusterName(submissionParameter.getClusterName());
            submitModelElement.addContent(advConfModel.exportToElement());
        }

        return submitModelElement;
    }

    public SparkSubmitModel applyFromElement(@NotNull Element rootElement)
            throws InvalidDataException{
        Attribute nilValueAttribute = new Attribute("Nil", "");
        Attribute falseValueAttribute = new Attribute("False", "false");
        Element emptyElement = new Element("Empty");

        return Optional.of(rootElement).filter(elem -> elem.getName().equals(SUBMISSION_CONTENT_NAME)).map(element -> {
            SubmissionTableModel tableModel = new SubmissionTableModel(
                    new String[]{"Key", "Value", ""});

            Optional.ofNullable(element.getChild(SUBMISSION_CONTENT_JOB_CONF))
                    .ifPresent(jobConfElem -> {
                        jobConfElem.getAttributes().forEach(attr -> {
                            tableModel.addRow(attr.getName(), attr.getValue());
                        });
                    });

            SparkSubmissionParameter parameter = new SparkSubmissionParameter(
                    Optional.ofNullable(element.getAttribute(SUBMISSION_ATTRIBUTE_CLUSTER_NAME))
                            .orElse(nilValueAttribute).getValue(),
                    Optional.ofNullable(element.getAttribute(SUBMISSION_ATTRIBUTE_IS_LOCAL_ARTIFACT))
                            .orElse(falseValueAttribute).getValue().toLowerCase().equals("true"),
                    Optional.ofNullable(element.getAttribute(SUBMISSION_ATTRIBUTE_ARTIFACT_NAME))
                            .orElse(nilValueAttribute).getValue(),
                    Optional.ofNullable(element.getAttribute(SUBMISSION_ATTRIBUTE_LOCAL_ARTIFACT_PATH))
                            .orElse(nilValueAttribute).getValue(),
                    Optional.ofNullable(element.getAttribute(SUBMISSION_ATTRIBUTE_FILE_PATH))
                            .orElse(nilValueAttribute).getValue(),
                    Optional.ofNullable(element.getAttribute(SUBMISSION_ATTRIBUTE_CLASSNAME))
                            .orElse(nilValueAttribute).getValue(),
                    Optional.ofNullable(element.getChild(SUBMISSION_CONTENT_REF_FILES))
                            .orElse(emptyElement).getContent().stream().map(cont -> ((Text) cont).getText()).collect
                            (Collectors.toList()),
                    Optional.ofNullable(element.getChild(SUBMISSION_CONTENT_REF_JARS))
                            .orElse(emptyElement).getContent().stream().map(cont -> ((Text) cont).getText()).collect
                            (Collectors.toList()),
                    Optional.ofNullable(element.getChild(SUBMISSION_CONTENT_COMMAND_LINE_ARGS))
                            .orElse(emptyElement).getContent().stream().map(cont -> ((Text) cont).getText()).collect
                            (Collectors.toList()),
                    tableModel.getJobConfigMap()
            );

            setSubmissionParameters(parameter);
            this.tableModel = tableModel;
            initializeTableModel(tableModel);

            Optional<SparkSubmitAdvancedConfigModel> advOpt = Optional.ofNullable(element.getChild(SUBMISSION_CONTENT_SSH_CERT))
                    .map(advConfElem -> {
                        SparkSubmitAdvancedConfigModel advConfModel = new SparkSubmitAdvancedConfigModel();
                        advConfModel.setClusterName(parameter.getClusterName());
                        return advConfModel.factoryFromElement(advConfElem);
                    });

            if (advOpt.isPresent()) {
                setAdvancedConfigModel(advOpt.get());
            } else {
                setAdvancedConfigModel(new SparkSubmitAdvancedConfigModel());
            }

            return this;
        }).orElse(this);
    }
}
