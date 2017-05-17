Feature: UI Logger Appender for log4j test
  Scenario: append calling test
    Given send ERROR 'ui helper check'
    Then get the append call with event message 'ui helper check'