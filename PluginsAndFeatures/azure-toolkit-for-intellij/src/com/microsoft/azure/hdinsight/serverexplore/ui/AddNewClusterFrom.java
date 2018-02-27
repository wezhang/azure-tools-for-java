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
package com.microsoft.azure.hdinsight.serverexplore.ui;

import com.intellij.CommonBundle;
import com.intellij.ide.BrowserUtil;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.ui.DialogWrapper;
import com.microsoft.azure.hdinsight.common.mvc.SettableControl;
import com.microsoft.azure.hdinsight.serverexplore.AddNewClusterCtrlProvider;
import com.microsoft.azure.hdinsight.serverexplore.AddNewClusterModel;
import com.microsoft.azure.hdinsight.serverexplore.hdinsightnode.HDInsightRootModule;
import com.microsoft.azuretools.telemetry.AppInsightsClient;
import com.microsoft.intellij.hdinsight.messages.HDInsightBundle;
import com.microsoft.intellij.rxjava.IdeaSchedulers;
import com.microsoft.tooling.msservices.model.storage.BlobContainer;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AddNewClusterFrom extends DialogWrapper implements SettableControl<AddNewClusterModel> {
    @Nullable
    private Project project;

    private JPanel addNewClusterPanel;
    private JTextField clusterNameFiled;
    private JTextField userNameField;
    private JPasswordField passwordField;
    private JTextField errorMessageField;
    private JTextField storageNameField;
    private JTextArea storageKeyTextField;
    private JLabel clusterNameLabel;
    private JLabel storageNameLabel;
    private JLabel storageKeyLabel;
    private JLabel userNameLabel;
    private JLabel passwordLabel;
    private JComboBox<BlobContainer> containersComboBox;
    private JLabel storageContainerLabel;

    @NotNull
    private HDInsightRootModule hdInsightModule;

    private static final String HELP_URL = "https://go.microsoft.com/fwlink/?linkid=866472";

    public AddNewClusterFrom(@Nullable final Project project, @NotNull HDInsightRootModule hdInsightModule) {
        super(project, true);
        this.project = project;

        myHelpAction = new HelpAction();

        init();
        this.hdInsightModule = hdInsightModule;

        this.setTitle("Link A New HDInsight Cluster");

        errorMessageField.setBackground(this.addNewClusterPanel.getBackground());
        errorMessageField.setBorder(BorderFactory.createEmptyBorder());

        this.setModal(true);

        storageKeyTextField.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                refreshContainers();
            }
        });

        storageNameField.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                refreshContainers();
            }
        });
    }

    private AddNewClusterCtrlProvider prepareCtrl() {
        AddNewClusterModel current = new AddNewClusterModel();

        getData(current);

        return new AddNewClusterCtrlProvider(current);
    }

    private void refreshContainers() {
        prepareCtrl()
                .refreshContainers()
                .subscribeOn(IdeaSchedulers.processBarVisibleAsync(this.project, "Getting storage account containers..."))
                .subscribe(this::setData);
    }

    @Override
    public void setData(@NotNull AddNewClusterModel data) {
        // Data -> Components

        // Text fields
        clusterNameFiled.setText(data.getClusterName());
        clusterNameLabel.setText(data.getClusterNameLabelTitle());
        userNameField.setText(data.getUserName());
        userNameLabel.setText(data.getUserNameLabelTitle());
        passwordField.setText(data.getPassword());
        passwordLabel.setText(data.getPasswordLabelTitle());
        storageNameField.setText(data.getStorageName());
        storageKeyTextField.setText(data.getStorageKey());
        errorMessageField.setText(data.getErrorMessage());

        // Combo box
        containersComboBox.removeAllItems();
        data.getContainers().forEach(containersComboBox::addItem);
        containersComboBox.setSelectedItem(data.getSelectedContainer());
    }

    @Override
    public void getData(@NotNull AddNewClusterModel data) {
        // Components -> Data
        data.setClusterName(clusterNameFiled.getText())
            .setClusterNameLabelTitle(clusterNameLabel.getText())
            .setUserName(userNameField.getText())
            .setUserNameLabelTitle(userNameLabel.getText())
            .setPassword(String.valueOf(passwordField.getPassword()))
            .setPasswordLabelTitle(passwordLabel.getText())
            .setStorageName(storageNameField.getText())
            .setStorageKey(storageKeyTextField.getText())
            .setErrorMessage(errorMessageField.getText())
            .setSelectedContainer((BlobContainer) containersComboBox.getSelectedItem())
            .setContainers(IntStream.range(0, containersComboBox.getItemCount())
                                    .mapToObj(i -> containersComboBox.getItemAt(i))
                                    .collect(Collectors.toList()));

    }

    private class HelpAction extends AbstractAction {
        private HelpAction() {
            this.putValue("Name", CommonBundle.getHelpButtonText());
        }

        public void actionPerformed(ActionEvent e) {
            AddNewClusterFrom.this.doHelpAction();
        }
    }

    @Override
    protected void doHelpAction() {
        BrowserUtil.browse(HELP_URL);
    }

    @Override
    protected void doOKAction() {
        prepareCtrl()
                .validateAndAdd()
                .subscribeOn(IdeaSchedulers.processBarVisibleAsync(this.project, "Validating the cluster settings..."))
                .doOnNext(this::setData)
                .map(AddNewClusterModel::getErrorMessage)
                .filter(StringUtils::isEmpty)
                .observeOn(IdeaSchedulers.dispatchThread())     // UI operation needs to be in dispatch thread
                .subscribe(toUpdate -> {
                    hdInsightModule.refreshWithoutAsync();
                    AppInsightsClient.create(HDInsightBundle.message("HDInsightAddNewClusterAction"), null);

                    super.doOKAction();
                });
    }

    @NotNull
    @Override
    protected Action[] createActions() {
        return new Action[] { getOKAction(), getCancelAction(), getHelpAction() };
    }

    @NotNull
    @Override
    protected Action[] createLeftSideActions() {
        return new Action[0];
    }

    @Nullable
    @Override
    protected JComponent createCenterPanel() {
        return addNewClusterPanel;
    }
}


