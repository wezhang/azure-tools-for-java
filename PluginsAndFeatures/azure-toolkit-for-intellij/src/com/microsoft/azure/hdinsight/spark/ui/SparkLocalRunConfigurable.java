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

import com.intellij.execution.ui.CommonJavaParametersPanel;
import com.intellij.openapi.ui.LabeledComponent;
import com.intellij.openapi.ui.TextFieldWithBrowseButton;
import com.microsoft.azure.hdinsight.spark.common.SparkLocalRunConfigurableModel;

import javax.swing.*;

public class SparkLocalRunConfigurable {
    private JPanel myWholePanel;
    private CommonJavaParametersPanel myCommonProgramParameters;
    private LabeledComponent myMainClass;
    private JCheckBox myParallelExecutionCheckbox;
    private TextFieldWithBrowseButton myWinutilsPathTextFieldWithBrowserButton;

    private void createUIComponents() {
        // TODO: place custom component creation code here
    }

    public void setData(SparkLocalRunConfigurableModel data) {
        myParallelExecutionCheckbox.setSelected(data.isIsParallelExecution());
    }

    public void getData(SparkLocalRunConfigurableModel data) {
        data.setIsParallelExecution(myParallelExecutionCheckbox.isSelected());
    }

    public boolean isModified(SparkLocalRunConfigurableModel data) {
        if (myParallelExecutionCheckbox.isSelected() != data.isIsParallelExecution()) return true;
        return false;
    }
}
