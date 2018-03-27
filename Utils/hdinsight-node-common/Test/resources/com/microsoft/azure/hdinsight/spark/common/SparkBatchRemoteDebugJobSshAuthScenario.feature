Feature: SparkBatchRemoteDebugJobSshAuth tests

  Scenario: isValid() unit tests
    Then checking isValid should match the following combinations
      | authType    | userName | password | keyFile        | isValid |
      | UsePassword |          | abc      |                | false   |
      | UseKeyFile  |          |          |                | false   |
      | UsePassword | anyUser  |          |                | false   |
      | UsePassword | anyUser  | abc      |                | true    |
      | UseKeyFile  | anyUser  |          |                | false   |
      | UseKeyFile  | anyUser  |          | nonexist.file  | false   |
      | UseKeyFile  | anyUser  |          | pom.xml        | true    |
