Feature: Criteria computations
  As a website owner
  I want to create better clustering of user submitted data
  So I increase the average "findability" of members

  Scenario: Merge and compute preferred variants from multiple criteria sources
    Given a file containing the following 'skill'
      | id | term        |
      | 1  | Java Script |
      | 2  | javascript  |
      | 3  | JavaScript  |
      | 4  | JavaScript  |
      | 5  | JavaScript  |
      | 6  | JavaScript  |
      | 7  | java-script |
      | 8  | java_script |
      | 9  | jāvascript  |
      | 10 | java/script |
      | 11 | php         |
      | 12 | PHP         |
      | 13 | PHP         |
    And a file containing the following 'location'
      | id | term  |
      | 14 | paris |
      | 15 | paris |
      | 16 | Paris |
      | 17 | Paris |
      | 18 | Paris |
      | 19 | PARIS |
      | 20 | PARIS |
    When I run the criteria job
    Then the criteria index should contains the following documents
      | preferred | normalized | origin      | type     |
      | false     | javascript | Java Script | skill    |
      | true      | javascript | JavaScript  | skill    |
      | false     | paris      | PARIS       | location |
      | true      | php        | PHP         | skill    |
      | true      | paris      | Paris       | location |
      | false     | javascript | java-script | skill    |
      | false     | javascript | java/script | skill    |
      | false     | javascript | java_script | skill    |
      | false     | javascript | javascript  | skill    |
      | false     | javascript | jāvascript  | skill    |
      | false     | paris      | paris       | location |
      | false     | php        | php         | skill    |
