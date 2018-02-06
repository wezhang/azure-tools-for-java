Feature: Livy Interactive Session Tests

  Scenario: Session.create() IT positive case with mocked http server
    Given setup a mock livy interactive service for POST request '/sessions' to return '{"id":6,"appId":null,"owner":null,"proxyUser":null,"state":"starting","kind":"spark","appInfo":{"driverLogUrl":null,"sparkUiUrl":null},"log":[]}' with status code 200
    And create a livy Spark interactive session instance with name 'testSparkREPL'
    Then check the returned livy interactive session after creating should be
      | id        | 6 |

  Scenario: Session.create() IT negative case with mocked http server
    Given setup a mock livy interactive service for POST request '/sessions' to return 'Bad Request' with status code 400
    And create a livy Spark interactive session instance with name 'testSparkREPL'
    Then check the HttpResponseException(400) when creating livy interactive session after creating should be thrown

  Scenario: Session.getAppId() IT positive case with mocked http server
    Given setup a mock livy interactive service for POST request '/sessions' to return '{"id":6,"appId":null,"owner":null,"proxyUser":null,"state":"starting","kind":"spark","appInfo":{"driverLogUrl":null,"sparkUiUrl":null},"log":[]}' with status code 200
    And setup a mock livy interactive service for GET request '/sessions/6' to return '{"id":6,"appId":"application_1517029729598_0086","owner":null,"proxyUser":null,"state":"idle","kind":"spark","appInfo":{"driverLogUrl":"https://zhwe-spkdbg.azurehdinsight.net/yarnui/10.0.0.8/node/containerlogs/container_e04_1517029729598_0086_01_000001/livy","sparkUiUrl":"https://zhwe-spkdbg.azurehdinsight.net/yarnui/hn/proxy/application_1517029729598_0086/"},"log":[]}' with status code 200
    And create a livy Spark interactive session instance with name 'testSparkREPL'
    Then check the returned livy interactive session after creating should be
      | id        | 6 |
    Then check getting app ID with waiting for livy interactive session application run should be 'application_1517029729598_0086'

  Scenario: Session.create() IT real case
    Given create a real livy Spark interactive session instance with name 'testSparkREPL'
    Then check getting app ID with waiting for livy interactive session application run should be 'abc'
