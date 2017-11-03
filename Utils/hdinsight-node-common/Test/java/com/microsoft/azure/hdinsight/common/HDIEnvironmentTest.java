package com.microsoft.azure.hdinsight.common;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        plugin = {"html:target/cucumber"},
        name = "HDIEnvironment.*"
)

public class HDIEnvironmentTest {
}
