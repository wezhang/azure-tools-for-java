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

import com.intellij.application.options.ModuleDescriptionsComboBox;
import com.intellij.execution.configurations.ConfigurationUtil;
import com.intellij.execution.configurations.JavaParameters;
import com.intellij.execution.ui.ClassBrowser;
import com.intellij.execution.ui.CommonJavaParametersPanel;
import com.intellij.execution.ui.ConfigurationModuleSelector;
import com.intellij.execution.ui.DefaultJreSelector;
import com.intellij.execution.util.JreVersionDetector;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.ui.ComponentWithBrowseButton;
import com.intellij.openapi.ui.LabeledComponent;
import com.intellij.openapi.ui.TextFieldWithBrowseButton;
import com.intellij.psi.JavaCodeFragment;
import com.intellij.psi.PsiClass;
import com.intellij.psi.PsiElement;
import com.intellij.psi.util.PsiMethodUtil;
import com.intellij.ui.EditorTextField;
import com.intellij.ui.EditorTextFieldWithBrowseButton;
import com.intellij.util.PathUtil;
import com.intellij.util.ui.UIUtil;
import com.microsoft.azure.hdinsight.spark.common.SparkLocalRunConfigurableModel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class SparkLocalRunConfigurable {
    private JPanel myWholePanel;
    private CommonJavaParametersPanel myCommonProgramParameters;
    private LabeledComponent<EditorTextFieldWithBrowseButton> myMainClass;
    private LabeledComponent<ModuleDescriptionsComboBox> myModule;
    private JCheckBox myParallelExecutionCheckbox;
    private TextFieldWithBrowseButton myWinutilsPathTextFieldWithBrowserButton;

    @Nullable
    private JComponent myAnchor;
    @NotNull
    private final ConfigurationModuleSelector myModuleSelector;
    @NotNull
    private final JreVersionDetector myVersionDetector;
    @NotNull
    private final Project myProject;

    public SparkLocalRunConfigurable(@NotNull final Project project) {
        this.myProject = project;
        myModuleSelector = new ConfigurationModuleSelector(project, myModule.getComponent());
//        myJrePathEditor.setDefaultJreSelector(DefaultJreSelector.fromSourceRootsDependencies(myModule.getComponent(), getMainClassField()));
        myCommonProgramParameters.setModuleContext(myModuleSelector.getModule());
        myModule.getComponent().addActionListener(e -> myCommonProgramParameters.setModuleContext(myModuleSelector.getModule()));
        ClassBrowser.createApplicationClassBrowser(project, myModuleSelector).setField(getMainClassField());
        myVersionDetector = new JreVersionDetector();

        myAnchor = UIUtil.mergeComponentsWithAnchor(myMainClass, myCommonProgramParameters, myModule);
    }

    @NotNull
    public EditorTextFieldWithBrowseButton getMainClassField() {
        return myMainClass.getComponent();
    }

    private void createUIComponents() {
        myMainClass = new LabeledComponent<>();
        myMainClass.setComponent(new EditorTextFieldWithBrowseButton(myProject, true, (declaration, place) -> {
            if (declaration instanceof PsiClass) {
                final PsiClass aClass = (PsiClass)declaration;
                if (ConfigurationUtil.MAIN_CLASS.value(aClass) && PsiMethodUtil.findMainMethod(aClass) != null ||
                        place.getParent() != null &&
                                myModuleSelector.findClass(((PsiClass)declaration).getQualifiedName()) != null) {
                    return JavaCodeFragment.VisibilityChecker.Visibility.VISIBLE;
                }
            }
            return JavaCodeFragment.VisibilityChecker.Visibility.NOT_VISIBLE;
        }));
    }

    public void setData(@NotNull SparkLocalRunConfigurableModel data) {
        // Data -> Component
        myParallelExecutionCheckbox.setSelected(data.isIsParallelExecution());
        myMainClass.getComponent().setText(data.getRunClass());
        myCommonProgramParameters.reset(data);
    }

    public void getData(@NotNull SparkLocalRunConfigurableModel data) {
        // Component -> Data
        data.setIsParallelExecution(myParallelExecutionCheckbox.isSelected());
        data.setRunClass(myMainClass.getComponent().getText());
        myCommonProgramParameters.applyTo(data);
    }

    public boolean isModified(@NotNull SparkLocalRunConfigurableModel data) {
        if (myParallelExecutionCheckbox.isSelected() != data.isIsParallelExecution()) {
            return true;
        }

        return false;
    }
}
