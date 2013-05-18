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
    When I run the "copy" job
    Then the output file should contain the following lines
      | term       | id |
      | javascript | 1  |
      | javascript | 2  |
      | javascript | 3  |
      | javascript | 4  |
      | javascript | 5  |
      | javascript | 6  |
      | javascript | 7  |
      | javascript | 8  |
      | javascript | 9  |
      | javascript | 10 |
