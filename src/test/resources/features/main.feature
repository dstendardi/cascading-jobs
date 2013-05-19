Feature: main

  Scenario: simple_copy
    Given a file containing the following lines
      | term        | id    |
      | Java Script | 1     |
      | javascript  | 2     |
      | JavaScript  | 3     |
      | JavaScript  | 4     |
      | JavaScript  | 5     |
      | JavaScript  | 6     |
      | java-script | 7     |
      | java_script | 8     |
      | jāvascript  | 9     |
      | java/script | 10    |
      | php         | 11    |
      | PHP         | 12    |
      | PHP         | 13    |
    When I run the "copy" job
    Then the output file should contain the following lines
      | preferred  | normalized |
      | JavaScript | javascript  |
      | PHP        | php         |



