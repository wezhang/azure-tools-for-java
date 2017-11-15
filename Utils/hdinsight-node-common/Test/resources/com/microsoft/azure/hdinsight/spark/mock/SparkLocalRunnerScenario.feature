Feature: Spark Local Runner Integration Test

  Scenario: Mocked default folder read
    Given locally run job 'com.microsoft.azure.hdinsight.spark.mock.jobapp.WordCountTest' with args
      | wasb:///word_count_input.txt |
    Then locally run stand output should be
      | a,1      |
      | mocked,1 |
      | fs,1     |
      | with,1   |
      | Spark,1  |
      | run,1    |
      | Hello,1  |
      | local,2  |
      | world,1  |