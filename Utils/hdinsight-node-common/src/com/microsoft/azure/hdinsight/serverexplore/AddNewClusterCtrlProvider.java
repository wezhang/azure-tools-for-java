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
 * SOFTWARE.
 */

package com.microsoft.azure.hdinsight.serverexplore;

import com.microsoft.azure.hdinsight.common.ClusterManagerEx;
import com.microsoft.azure.hdinsight.sdk.cluster.HDInsightAdditionalClusterDetail;
import com.microsoft.azure.hdinsight.sdk.common.AuthenticationException;
import com.microsoft.azure.hdinsight.sdk.storage.HDStorageAccount;
import com.microsoft.azure.hdinsight.spark.jobs.JobUtils;
import com.microsoft.azuretools.azurecommons.helpers.NotNull;
import com.microsoft.azuretools.azurecommons.helpers.StringHelper;
import com.microsoft.tooling.msservices.helpers.azure.sdk.StorageClientSDKManager;
import com.microsoft.tooling.msservices.model.storage.BlobContainer;
import com.microsoft.tooling.msservices.model.storage.ClientStorageAccount;
import org.apache.commons.lang3.StringUtils;
import rx.Observable;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Optional;

public class AddNewClusterCtrlProvider {
    private static final String URL_PREFIX = "https://";

    @NotNull
    private AddNewClusterModel model;

    public AddNewClusterCtrlProvider(@NotNull AddNewClusterModel model) {
        this.model = model;
    }

    @NotNull
    public AddNewClusterModel getModel() {
        return model;
    }

    //format input string
    private static String getClusterName(String userNameOrUrl) {
        if (userNameOrUrl.startsWith(URL_PREFIX)) {
            return StringHelper.getClusterNameFromEndPoint(userNameOrUrl);
        } else {
            return userNameOrUrl;
        }
    }

    public Observable<AddNewClusterModel> refreshContainers() {
        return Observable.fromCallable(() -> {
            AddNewClusterModel toUpdate = (AddNewClusterModel) getModel().clone();

            toUpdate.setContainers(null);

            if (StringUtils.isNotBlank(getModel().getStorageName()) && StringUtils.isNotBlank(getModel().getStorageKey())) {
                try {
                    ClientStorageAccount storageAccount = new ClientStorageAccount(getModel().getStorageName());
                    storageAccount.setPrimaryKey(getModel().getStorageKey());

                    toUpdate.setContainers(StorageClientSDKManager.getManager().getBlobContainers(storageAccount.getConnectionString()));
                } catch (Exception ex) {
                    return toUpdate.setErrorMessage("Can't get storage containers, check if the key matches");
                }
            }

            return toUpdate.setErrorMessage(null);
        });
    }

    public Observable<AddNewClusterModel> validateAndAdd() {
        return Observable.fromCallable(() -> {
            AddNewClusterModel toUpdate = (AddNewClusterModel) getModel().clone();

            String clusterNameOrUrl = getModel().getClusterName();
            String userName = getModel().getUserName();
            String storageName = getModel().getStorageName();
            String storageKey = getModel().getStorageKey();
            String password = getModel().getPassword();
            BlobContainer selectedContainer = getModel().getSelectedContainer();

            // Incomplete data check
            if (StringHelper.isNullOrWhiteSpace(clusterNameOrUrl) ||
                    StringHelper.isNullOrWhiteSpace(userName) ||
                    StringHelper.isNullOrWhiteSpace(password)) {
                String highlightPrefix = "* ";

                if (!toUpdate.getClusterNameLabelTitle().startsWith(highlightPrefix)) {
                    toUpdate.setClusterNameLabelTitle(highlightPrefix + toUpdate.getClusterNameLabelTitle());
                }

                if (!toUpdate.getUserNameLabelTitle().startsWith(highlightPrefix)) {
                    toUpdate.setUserNameLabelTitle(highlightPrefix + toUpdate.getUserNameLabelTitle());
                }

                if (!toUpdate.getPasswordLabelTitle().startsWith(highlightPrefix)) {
                    toUpdate.setPasswordLabelTitle(highlightPrefix + toUpdate.getPasswordLabelTitle());
                }

                return toUpdate.setErrorMessage("All (*) fields are required.");
            }

            String clusterName = getClusterName(clusterNameOrUrl);

            // Cluster name check
            if (clusterName == null) {
                return toUpdate.setErrorMessage("Wrong cluster name or endpoint");
            }

            // Duplication check
            if (ClusterManagerEx.getInstance().getHdinsightAdditionalClusterDetails().stream().anyMatch(clusterDetail ->
                    clusterDetail.getName().equals(clusterName))) {
                return toUpdate.setErrorMessage("Cluster already exists in linked list");
            }

            // Storage access check
            HDStorageAccount storageAccount = null;
            if (StringUtils.isNotEmpty(storageName)) {
                ClientStorageAccount storageAccountClient = new ClientStorageAccount(storageName);
                storageAccountClient.setPrimaryKey(storageKey);

                // Storage Key check
                try {
                    StorageClientSDKManager.getCloudStorageAccount(storageAccountClient.getConnectionString());
                } catch (Exception ex) {
                    return toUpdate.setErrorMessage("Storage key doesn't match the account.");
                }

                // Containers selection check
                if (selectedContainer == null) {
                    return toUpdate.setErrorMessage("The storage container isn't selected");
                }

                storageAccount = new HDStorageAccount(
                        null,
                        ClusterManagerEx.getInstance().getBlobFullName(storageName),
                        storageKey,
                        false,
                        selectedContainer.getName());
            }

            HDInsightAdditionalClusterDetail hdInsightAdditionalClusterDetail =
                    new HDInsightAdditionalClusterDetail(clusterName, userName, password, storageAccount);

            // Account certificate check
            try {
                JobUtils.authenticate(hdInsightAdditionalClusterDetail);
            } catch (AuthenticationException authErr) {
                return toUpdate.setErrorMessage("Authentication Error: " + Optional.ofNullable(authErr.getMessage())
                        .filter(msg -> !msg.isEmpty())
                        .orElse("Wrong username/password") +
                        " (" + authErr.getErrorCode() + ")");
            } catch (Exception ex) {
                return toUpdate.setErrorMessage("Authentication Error: " + ex.getMessage());
            }

            // No issue
            ClusterManagerEx.getInstance().addHDInsightAdditionalCluster(hdInsightAdditionalClusterDetail);

            return toUpdate.setErrorMessage(null);
        });
    }
}
