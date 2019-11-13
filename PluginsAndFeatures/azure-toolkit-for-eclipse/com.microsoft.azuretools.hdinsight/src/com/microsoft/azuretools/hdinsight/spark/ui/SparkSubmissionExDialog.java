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
package com.microsoft.azuretools.hdinsight.spark.ui;

import static com.microsoft.azuretools.telemetry.TelemetryConstants.HDINSIGHT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IWorkspace;
import org.eclipse.core.resources.IWorkspaceRoot;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.jdt.core.ICompilationUnit;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jdt.core.IMethod;
import org.eclipse.jdt.core.IPackageFragment;
import org.eclipse.jdt.core.IPackageFragmentRoot;
import org.eclipse.jdt.core.IType;
import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ICellModifier;
import org.eclipse.jface.viewers.ILabelProviderListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TextCellEditor;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.accessibility.AccessibleAdapter;
import org.eclipse.swt.accessibility.AccessibleEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import com.microsoft.azure.hdinsight.common.CallBack;
import com.microsoft.azure.hdinsight.common.ClusterManagerEx;
import com.microsoft.azure.hdinsight.common.CommonConst;
import com.microsoft.azure.hdinsight.sdk.cluster.ClusterDetail;
import com.microsoft.azure.hdinsight.sdk.cluster.HDInsightAdditionalClusterDetail;
import com.microsoft.azure.hdinsight.sdk.cluster.IClusterDetail;
import com.microsoft.azure.hdinsight.spark.common.SparkSubmissionParameter;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.Nullable;
import com.microsoft.azuretools.azurecommons.helpers.StringHelper;
import com.microsoft.azuretools.azureexplorer.forms.AddNewHDInsightReaderClusterForm;
import com.microsoft.azuretools.core.utils.Messages;
import com.microsoft.azuretools.core.utils.PluginUtil;
import com.microsoft.azuretools.hdinsight.Activator;
import com.microsoft.azuretools.hdinsight.projects.HDInsightProjectNature;
import com.microsoft.azuretools.hdinsight.spark.common2.SparkSubmitModel;
import com.microsoft.azuretools.telemetry.AppInsightsClient;
import com.microsoft.azuretools.telemetrywrapper.EventType;
import com.microsoft.azuretools.telemetrywrapper.EventUtil;

import static org.eclipse.swt.SWT.CENTER;
import static org.eclipse.swt.SWT.FILL;
import static org.eclipse.swt.SWT.TOP;

public class SparkSubmissionExDialog extends Dialog {
	static class GridDataBuilder {
		// Default Grid Data settings
		private GridData gridData = new GridData(FILL, CENTER, true, false, 1, 1);

		public GridDataBuilder widthHint(final int hint) {
			gridData.widthHint = hint;
			return this;
		}

		public GridDataBuilder heightHint(final int hint) {
			gridData.heightHint = hint;
			return this;
		}

		public GridDataBuilder horizontalIndent(final int indent) {
			gridData.horizontalIndent = indent;
			return this;
		}

		public GridDataBuilder verticalAlignment(final int align) {
			gridData.verticalAlignment = align;
			return this;
		}

		public GridDataBuilder span(final int horizontalSpan, final int verticalSpan) {
			gridData.horizontalSpan = horizontalSpan;
			gridData.verticalSpan = verticalSpan;
			return this;
		}

		public GridData build() {
			return gridData;
		}
	}

	private final int margin = 10;
	private static final String DIALOG_TITLE = "Spark Submission";
	private static final String[] COLUMN_NAMES = { "Key", "Value" };
	private static final String JAVA_NATURE_ID = "org.eclipse.jdt.core.javanature";
	
	private int row = 0;
	private Combo clustersListComboBox;
	private Label hdiReaderErrorMsgLabel;
	private Link hdiReaderLink;

	private Button projectArtifactRadioButton;
	private Button localArtifactRadioButton;
	private Combo projectArtifactSelectComboBox;
	private Text localArtifactInput;
	private Button localArtifactBrowseButton;

	private Table jobConfigurationTable;
	private TableViewer tableViewer;
	private Combo mainClassCombo;
	private Text commandLineTextField;
	private Text referencedJarsTextField;
	private Text referencedFilesTextField;

	private SparkSubmitModel submitModel;
	private IProject myProject;
	private CallBack callBack;
	private List<IClusterDetail> cachedClusterDetails;
	private Map<String, Object> jobConfigMap = new HashMap<String, Object>();

	public SparkSubmissionExDialog(Shell parentShell, @NotNull List<IClusterDetail> cachedClusterDetails,
			@Nullable IProject project, @Nullable CallBack callBack) {
		super(parentShell);
		this.cachedClusterDetails = cachedClusterDetails;
		this.myProject = project;
		this.callBack = callBack;
		submitModel = new SparkSubmitModel(cachedClusterDetails);
	}
	
	@Override
	protected void configureShell(Shell newShell) {
		super.configureShell(newShell);
		newShell.setText(DIALOG_TITLE);
	}

	@Override
	protected Control createButtonBar(Composite parent) {
		Control buttonBar = super.createButtonBar(parent);
		getButton(Dialog.OK).setText("Submit");
		return buttonBar;
	}

	@Override
	protected Control createContents(Composite parent) {
		final Composite root = new Composite(parent, SWT.NONE);
		root.setLayout(new GridLayout(2, false));
		root.setLayoutData(new GridDataBuilder().widthHint(600).build());

		final Label clusterListLabel = new Label(root, SWT.LEFT);
		clusterListLabel.setText("Cluster Name:");

		clustersListComboBox = new Combo(root, SWT.READ_ONLY);
		clustersListComboBox.setLayoutData(new GridDataBuilder().build());
		clustersListComboBox.setToolTipText(
				"The HDInsight Spark cluster you want to submit your application to. Only Linux cluster is supported.");
		clustersListComboBox.addModifyListener(event -> {
			IClusterDetail clusterDetail = getSelectedCluster(clustersListComboBox.getText());
			if (clusterDetail != null && ClusterManagerEx.getInstance().isHdiReaderCluster(clusterDetail)) {
				showHdiReaderErrors(true);
			} else {
				showHdiReaderErrors(false);
			}
		});

		// Execute "select the first item" operation after the dialog opened
		Display.getDefault().asyncExec(() -> {
			for (IClusterDetail clusterDetail : cachedClusterDetails) {
	            clustersListComboBox.add(clusterDetail.getTitle());
	            clustersListComboBox.setData(clusterDetail.getTitle(), clusterDetail);
			}
			if (cachedClusterDetails.size() > 0) {
				// Send SWT.Modify event after select the first item
				clustersListComboBox.select(0);
			}
		});

		//Add blank label as a placeholder
		final Label placeholderLabel = new Label(root, SWT.LEFT);
		placeholderLabel.setVisible(false);
		final Composite warningWithLink = new Composite(root, SWT.NONE);
		warningWithLink.setLayout(new GridLayout(2, false));
		warningWithLink.setLayoutData(new GridDataBuilder().build());
		
		// Add warning message and link cluster button composite
		hdiReaderErrorMsgLabel = new Label(warningWithLink, SWT.LEFT);
		hdiReaderErrorMsgLabel.setText(
				"No Ambari permission to submit job to the selected cluster...");
		hdiReaderErrorMsgLabel.setToolTipText(
				"No Ambari permission to submit job to the selected cluster. Please ask the cluster owner or user "
				+ "access administrator to upgrade your role to HDInsight Cluster Operator in the Azure Portal, or "
				+ "link to the selected cluster.");
		hdiReaderLink = new Link(warningWithLink, SWT.NONE);
		hdiReaderLink.setText("<a href=\" \">Link this cluster</a>");
		hdiReaderLink.addListener(SWT.Selection, e -> {
			IClusterDetail selectedClusterDetail = getSelectedCluster(clustersListComboBox.getText());
			if (selectedClusterDetail != null && ClusterManagerEx.getInstance().isHdiReaderCluster(selectedClusterDetail)) {
				String defaultStorageRootPath = ((ClusterDetail) selectedClusterDetail).getDefaultStorageRootPath();
				
				AddNewHDInsightReaderClusterForm linkClusterForm = new AddNewHDInsightReaderClusterForm(
						PluginUtil.getParentShell(), null, (ClusterDetail) selectedClusterDetail) {
					protected void afterOkActionPerformed() {
						showHdiReaderErrors(false);
						HDInsightAdditionalClusterDetail linkedCluster = 
								(HDInsightAdditionalClusterDetail) ClusterManagerEx.getInstance().findClusterDetail(clusterDetail ->
										getSelectedLinkedHdiCluster(clusterDetail, selectedClusterDetail.getName()), true);
						if (linkedCluster != null) {
							linkedCluster.setDefaultStorageRootPath(defaultStorageRootPath);
							ClusterManagerEx.getInstance().updateHdiAdditionalClusterDetail(linkedCluster);
							
							// Display the HDI reader cluster as linked cluster
							Display.getDefault().asyncExec(() -> {
								int selectedClusterIndex = clustersListComboBox.indexOf(selectedClusterDetail.getTitle());
								clustersListComboBox.setItem(selectedClusterIndex, linkedCluster.getTitle());
								clustersListComboBox.setData(linkedCluster.getTitle(), linkedCluster);
							});
						}							
					}
				};
				linkClusterForm.open();
			}
		});

		// Hide HDInsight reader cluster error when dialog open at first time
		showHdiReaderErrors(false);
		
		// Radio button group for the artifact selection
		final Group artifactSelectGroup = new Group(root, SWT.NONE);
		artifactSelectGroup.setText("Select an Artifact to submit");
		artifactSelectGroup.setLayout(new GridLayout(2, false));
		artifactSelectGroup.setLayoutData(new GridDataBuilder().span(2, 2).build());

		// Project artifacts Radio button
		projectArtifactRadioButton = new Button(artifactSelectGroup, SWT.RADIO);
		projectArtifactRadioButton.setText("Artifact from Eclipse project:");
		projectArtifactRadioButton.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent ignored) {
				switchToProjectArtifactSelection(true);
			}
		});

		final String selectProjectArtifactTip = "The Artifact you want to use.";
		projectArtifactSelectComboBox = new Combo(artifactSelectGroup, SWT.READ_ONLY);
		projectArtifactSelectComboBox.setToolTipText(selectProjectArtifactTip);
		projectArtifactSelectComboBox.setLayoutData(new GridDataBuilder().widthHint(400).build());
		projectArtifactSelectComboBox.getAccessible().addAccessibleListener(new AccessibleAdapter() {
			public void getName(AccessibleEvent event) {
				event.result = projectArtifactRadioButton.getText();
			}
		});
		final String[] projects = getProjects();
		projectArtifactSelectComboBox.setItems(projects);
		if (myProject != null) {
			final String selectedProjectName = myProject.getName();
			for (int i = 0; i < projects.length; ++i) {
				if (projects[i].equalsIgnoreCase(selectedProjectName)) {
					projectArtifactSelectComboBox.select(i);
				}
			}
		} else if (projects.length > 0) {
			projectArtifactSelectComboBox.select(0);
		}

		// Local artifact
		localArtifactRadioButton = new Button(artifactSelectGroup, SWT.RADIO);
		localArtifactRadioButton.setText("Artifact from hard disk:");
		localArtifactRadioButton.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent ignored) {
				switchToProjectArtifactSelection(false);
			}
		});

		final Composite localArtifactSelection = new Composite(artifactSelectGroup, SWT.NONE);
		localArtifactSelection.setLayout(new GridLayout(2, false));
		localArtifactSelection.setLayoutData(new GridDataBuilder().build());

		final String localArtifactInputTip = "Input the local artifact path.";
		localArtifactInput = new Text(localArtifactSelection, SWT.LEFT | SWT.BORDER);
		localArtifactInput.setToolTipText(localArtifactInputTip);
		localArtifactInput.setLayoutData(new GridDataBuilder().build());
		localArtifactInput.getAccessible().addAccessibleListener(new AccessibleAdapter() {
			public void getName(AccessibleEvent event) {
				event.result = localArtifactRadioButton.getText();
			}
		});

		// Browser button to open file selection dialog
		localArtifactBrowseButton = new Button(localArtifactSelection, SWT.PUSH);
		localArtifactBrowseButton.setText("Browse");
		localArtifactBrowseButton.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {
				FileDialog dialog = new FileDialog(SparkSubmissionExDialog.this.getShell());
				String[] extensions = { "*.jar", "*.JAR" };
				dialog.setFilterExtensions(extensions);
				String file = dialog.open();
				if (file != null) {
					localArtifactInput.setText(file);
				}
			}
		});

		localArtifactBrowseButton.getAccessible().addAccessibleListener(new AccessibleAdapter() {
			public void getName(AccessibleEvent event) {
				event.result = localArtifactRadioButton.getText() + " open System File dialog to select artifact";
			}
		});

		// Default selection
		projectArtifactRadioButton.setSelection(true);
		switchToProjectArtifactSelection(true);

		// Main class input
		final Label sparkMainClassLabel = new Label(root, SWT.LEFT);
		sparkMainClassLabel.setText("Main class name");

		mainClassCombo = new Combo(root, SWT.DROP_DOWN);
		mainClassCombo.setToolTipText("Application's java/spark main class");
		mainClassCombo.setLayoutData(new GridDataBuilder().build());
		try {
			java.util.Set<String> classes = getClassesWithMainMethod();
		    String [] names = new String[classes.size()];
		    classes.toArray(names);
		    mainClassCombo.setItems(names);
		    mainClassCombo.select(0);
		} catch (CoreException e1) {
			Activator.getDefault().log("get main class list error", e1);
		}
	    
		// Job configuration
		final Label jobConfigurationLabel = new Label(root, SWT.LEFT);
		jobConfigurationLabel.setText("Job configurations");
		jobConfigurationLabel.setLayoutData(new GridDataBuilder().verticalAlignment(TOP).build());

		jobConfigurationTable = new Table(root, SWT.BORDER);
		jobConfigurationTable.setHeaderVisible(true);
		jobConfigurationTable.setLinesVisible(true);

		jobConfigurationTable.setLayout(new GridLayout(2, false));
		jobConfigurationTable.setLayoutData(new GridDataBuilder().heightHint(75).build());

		final TableColumn key = new TableColumn(jobConfigurationTable, SWT.FILL);
		key.setText(COLUMN_NAMES[0]);
		key.setWidth(150);

		final TableColumn value = new TableColumn(jobConfigurationTable, SWT.FILL);
		value.setText(COLUMN_NAMES[1]);
		value.setWidth(80);

		tableViewer = new TableViewer(jobConfigurationTable);
		tableViewer.setUseHashlookup(true);
		tableViewer.setColumnProperties(COLUMN_NAMES);

		final CellEditor[] editors = new CellEditor[2];

		editors[1] = new TextCellEditor(jobConfigurationTable);

		tableViewer.setCellEditors(editors);
		tableViewer.setContentProvider(new JobConfigurationContentProvider());
		tableViewer.setLabelProvider(new JobConfigurationLabelProvider());
		tableViewer.setCellModifier(new JobConfigurationCellModifier());
		initializeTable();

		final Label label = new Label(root, SWT.LEFT);
		label.setText("Command line arguments");
		final String commandLineTip = "Command line arguments used in your main class; multiple arguments should be split by space.";
		commandLineTextField = new Text(root, SWT.LEFT | SWT.BORDER);
		commandLineTextField.setToolTipText(commandLineTip);
		commandLineTextField.setLayoutData(new GridDataBuilder().build());

		final String refJarsTip = "Files to be placed on the java classpath; The path needs to be a Azure Blob Storage Path (path started with wasb://); Multiple paths should be split by semicolon (;)";
		final Label referencedJarsLabel = new Label(root, SWT.LEFT);
		referencedJarsLabel.setText("Referenced Jars");
		referencedJarsTextField = new Text(root, SWT.BORDER | SWT.LEFT);
		referencedJarsTextField.setToolTipText(refJarsTip);
		referencedJarsTextField.setLayoutData(new GridDataBuilder().build());

		final Label referencedFilesLabel = new Label(root, SWT.LEFT);
		referencedFilesLabel.setText("Referenced Files");
		final String refFilesTip = "Files to be placed in executor working directory. The path needs to be a Azure Blob Storage Path (path started with wasb://); Multiple paths should be split by semicolon (;) ";
		referencedFilesTextField = new Text(root, SWT.BORDER | SWT.LEFT);
		referencedFilesTextField.setToolTipText(refFilesTip);
		referencedFilesTextField.setLayoutData(new GridDataBuilder().build());

		return super.createContents(parent);
	}
	
	private void switchToProjectArtifactSelection(final boolean isProjectArtifact) {
		projectArtifactSelectComboBox.setEnabled(isProjectArtifact);
		localArtifactInput.setEnabled(!isProjectArtifact);
		localArtifactBrowseButton.setEnabled(!isProjectArtifact);
	}

	private void showHdiReaderErrors(boolean isVisible) {
		hdiReaderErrorMsgLabel.setVisible(isVisible);
		hdiReaderLink.setVisible(isVisible);
	}

	private class JobConfigurationContentProvider implements IStructuredContentProvider {
		@Override
		public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
		}

		@Override
		public void dispose() {
		}

		@Override
		public Object[] getElements(Object arg0) {
			return jobConfigMap.entrySet().toArray();
		}
	}

	private class JobConfigurationLabelProvider implements ITableLabelProvider {

		@Override
		public void removeListener(ILabelProviderListener arg0) {
		}

		@Override
		public boolean isLabelProperty(Object arg0, String arg1) {
			return false;
		}

		@Override
		public void dispose() {
		}

		@Override
		public void addListener(ILabelProviderListener arg0) {
		}

		@Override
		public String getColumnText(Object element, int colIndex) {
			Map.Entry<String, Object> keyValue = (Map.Entry<String, Object>) element;
			switch (colIndex) {
			case 0:
				return keyValue.getKey();
			case 1:
				return "" + keyValue.getValue();
			default:
				return "";
			}
		}

		@Override
		public Image getColumnImage(Object arg0, int arg1) {
			return null;
		}
	}

	private class JobConfigurationCellModifier implements ICellModifier {
		@Override
		public void modify(Object waEndpoint, String columnName, Object modifiedVal) {
			TableItem tblItem = (TableItem) waEndpoint;
			Map.Entry<String, Object> keyValue = (Map.Entry<String, Object>) tblItem.getData();
			if (columnName.equals(COLUMN_NAMES[1])) {
				String value = modifiedVal.toString();
				keyValue.setValue(value);
			}
			tableViewer.refresh();
		}

		public Object getValue(Object element, String property) {
			Map.Entry<String, Object> keyValue = (Map.Entry<String, Object>) element;

			if (property.equals(COLUMN_NAMES[0])) {
				return keyValue.getKey();
			} else if (property.equals(COLUMN_NAMES[1])) {
				return keyValue.getValue();
			}
			return "";
		}

		/**
		 * Determines whether a particular cell can be modified or not.
		 * 
		 * @return boolean
		 */
		@Override
		public boolean canModify(Object element, String property) {
			return property.equals(COLUMN_NAMES[1]);
		}
	}

	private void initializeTable() {
		for (int i = 0; i < SparkSubmissionParameter.defaultParameters.length; ++i) {
			jobConfigMap.put(SparkSubmissionParameter.defaultParameters[i].first(), SparkSubmissionParameter.defaultParameters[i].second());
		}
		tableViewer.setInput(jobConfigMap.entrySet());
	}

	@Nullable
	private IClusterDetail getSelectedCluster(@NotNull String title) {
		return (IClusterDetail) clustersListComboBox.getData(title);
	}
	
	private SparkSubmissionParameter constructSubmissionParameter() {
		IClusterDetail selectedClusterDetail = getSelectedCluster(clustersListComboBox.getText());

		String selectedArtifactName = projectArtifactSelectComboBox.getText();
		String className = mainClassCombo.getText().trim();
		String commandLine = commandLineTextField.getText().trim();
		String localArtifactPath = localArtifactInput.getText();
		String selectedClusterName = selectedClusterDetail.getName();

		List<String> referencedFileList = new ArrayList<>();
		for (String singleReferencedFile : referencedFilesTextField.getText().split(";")) {
			singleReferencedFile = singleReferencedFile.trim();
			if (!StringHelper.isNullOrWhiteSpace(singleReferencedFile)) {
				referencedFileList.add(singleReferencedFile);
			}
		}

		List<String> uploadedFilePathList = new ArrayList<>();
		for (String singleReferencedJars : referencedJarsTextField.getText().split(";")) {
			singleReferencedJars = singleReferencedJars.trim();
			if (!StringHelper.isNullOrWhiteSpace(singleReferencedJars)) {
				uploadedFilePathList.add(singleReferencedJars);
			}
		}

		List<String> argsList = new ArrayList<>();
		for (String singleArs : commandLine.split(" ")) {
			if (!StringHelper.isNullOrWhiteSpace(singleArs)) {
				argsList.add(singleArs.trim());
			}
		}

		return new SparkSubmissionParameter(selectedClusterName, localArtifactRadioButton.getSelection(),
				selectedArtifactName, localArtifactPath, null, className, referencedFileList, uploadedFilePathList,
				argsList, jobConfigMap);
	}

	private String[] getProjects() {
		IWorkspace workspace = ResourcesPlugin.getWorkspace();
		IWorkspaceRoot root = workspace.getRoot();
		String[] projects = null;
		ArrayList<String> projList = new ArrayList<String>();
		try {
			for (IProject wRoot : root.getProjects()) {
				if (wRoot.hasNature(HDInsightProjectNature.NATURE_ID)) {
					projList.add(wRoot.getProject().getName());
				}
			}
			projects = new String[projList.size()];
			projects = projList.toArray(projects);
		} catch (Exception e) {
			PluginUtil.displayErrorDialogAndLog(this.getShell(), "Project Selection Error",
					"Error occurred while selecting the project.", e);
		}
		return projects;
	}

	private java.util.Set<String> getClassesWithMainMethod() throws CoreException {
		java.util.Set<String> filterClassSet = new HashSet<String>();
		if (myProject != null && myProject.isNatureEnabled(JAVA_NATURE_ID)) {
			IJavaProject javaProject = JavaCore.create(myProject);
			
			if (javaProject == null) {
				return filterClassSet;
			}

			IPackageFragment[] packages = javaProject.getPackageFragments();
			if (packages == null || packages.length == 0) {
				return filterClassSet;
			}

			for (IPackageFragment mypackage : packages) {

				// get source type
				if (mypackage.getKind() == IPackageFragmentRoot.K_SOURCE) {
					for (ICompilationUnit unit : mypackage.getCompilationUnits()) {
						IType[] allTypes = unit.getAllTypes();
						final String unitName = unit.getElementName();
						for (IType type : allTypes) {
							IMethod[] methods = type.getMethods();
							for (IMethod method : methods) {
								if (method.getElementName().equalsIgnoreCase("main")) {

									String simpleClassName = type.getElementName();
									if (simpleClassName == null || simpleClassName.isEmpty()) {
										// remove .class suffix
										simpleClassName = unitName.substring(0, unitName.length() - 6);
									}
									
									// Handle duplicated Scala class
									if (simpleClassName.endsWith("$") && simpleClassName.length() > 1) {
										simpleClassName = simpleClassName.substring(0, simpleClassName.length() - 1);
									}
									
									if (mypackage.isDefaultPackage()) {
										filterClassSet.add(simpleClassName);
									} else {
										final String className = String.format("%s.%s", mypackage.getElementName(),
												simpleClassName);
										filterClassSet.add(className);
									}
								}
							}
						}
					}
				}
			}
		}
		return filterClassSet;
	}

	@Override
	protected void okPressed() {
		AppInsightsClient.create(Messages.SparkSubmissionButtonClickEvent, null);
		EventUtil.logEvent(EventType.info, HDINSIGHT, Messages.SparkSubmissionButtonClickEvent, null);
		submitModel.action(constructSubmissionParameter());
		super.okPressed();
	}

}
