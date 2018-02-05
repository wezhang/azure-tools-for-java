Feature: Livy Interactive Session Tests

  Scenario: Session.create() IT positive case with mocked http server
    Given setup a mock livy service for POST request '/sessions' to return '{}' with status code 200
    And create a livy Spark interactive session instance with name 'testSparkREPL'
    Then check the returned livy interactive session after creating should be
      | id        | |
      | appId     | |
