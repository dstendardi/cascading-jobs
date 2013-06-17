Feature: Criteria computations
  As a website owner
  I want to create better clustering of user submitted data
  So I increase the average "findability" of members

  @wip
  Scenario: Merge and compute preferred variants from multiple criteria sources
    Given a file containing the following 'skill'
      | id | term        |
      | 1  | Java        |
      | 2  | Java        |
      | 3  | java        |
      | 4  | Jâva        |
    And a file containing the following 'location'
      | id | term  |
      | 1  | paris |
      | 2  | Paris |
      | 3  | Paris |
      | 4  | java  |
      | 5  | java  |
    And a file containing the following 'company'
      | id | term  |
      | 1  | JAVA  |
    When I run the criteria job
    Then the criteria index should contains the following documents
      | preferred | normalized | origin      | type     |
      | true      | java       | Java        | skill    |
      | false     | java       | java        | skill    |
      | false     | java       | Jâva        | skill    |
      | true      | java       | java        | location |
      | true      | paris      | Paris       | location |
      | false     | paris      | paris       | location |
      | true      | java       | JAVA        | company  |
