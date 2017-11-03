package com.microsoft.azure.hdinsight.common;


public interface IEnvironment {
    String getClusterConnectionFormat();
    String getBlobFullNameFormat();
    String getPortal();
}
