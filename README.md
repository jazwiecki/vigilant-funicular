# Functional testing approach for Luigi ETL tasks

WIP research project. To use automated testing in a data warehouse codebase,
you need to express a truth about an ETL method's behavior using only SQL
(or at least I did, maybe you have another query language handy). If the
`T` in your ETL removes some tuples as part of a cleanup process, or you're
enriching data, it can be tricky (if not impossible) to write
integration/functional tests of the whole ETL process, because the results
of a query on the extracted data might not match the results of the loaded
data. One approach is to use a set of data fixtures, manually confirm some
fixed stare-and-compare values for queries, and hard-code those values into
tests. There are three problems with this approach:

1. Your enrichment might change, especially if you use third-party data
which can't be versioned, breaking your tests even though you didn't change
any code.
2. Data removed during cleanup might change, as you might add a new `user-agent`
to the list of excluded agents, or the third-party library you use to
enrich `user-agent` data might update the patterns it uses.
3. Let's say you need to tweak some logic for how the extracted data is
transformed, and you want to alter your test fixtures to make sure your
coverage doesn't go down. If you add a couple new tuples to your fixtures,
you have to re-calculate a bunch of other, unrelated queries.

Sure, you could pin your code to versioned third party libraries/data, but
you're _going_ to want to bump that version, and when you do, you're going
to have to update fixed values in a bunch of queries. Because the whole
organization is depending on the quality of this data, you're going to
exhaustively cross-check all the new results before fixing the new values,
and not just dump whatever number the failed tests logged post-ETL, right?

You _could_ do that or you could use a compact assertion vocabulary to
quickly describe the properties of all the columns in all the tables, which
would reflect the acceptance criteria of the data team and offer equivalent
coverage.

## Background

Prior work found data quality and balancing tests, run against destination
databases, were effective in identifying faults in transformation/load stages.
For the codebase in question, which picked up data delivered by the
extraction stage using tasks written in Python, run by [Luigi](https://github.com/spotify/luigi),
to be stored in RedShift, there was no good way to reason about exact counts
in the loaded data based on access to the extracted data, so I had to come
up with another way to validate assertions about the state of the loaded data.

## Approach

For each column in a table, you can get pretty good coverage on completeness,
consistency, and syntactic validity without getting into the weeds with
exact counts:

* Completeness can be covered by asserting that no records are empty, that
the sum of values in a numeric column is greater than zero, that you ended
up with less tuples than in the source data, etc
* Consistency can be tested by checking the statistical properties of
specific attributes matches expectations, that the portion of distinct
values isn't 0% or 100%, that some (but not _all_) records are empty, etc
* Syntactic validity is covered by some of the above, as well as checking
for type conversion errors (e.g. `NaN` strings, zero-value Epoch timestamps)

I wrote a couple SQL queries that would test these properties in the abstract,
and coupled them with an "assertion" file for a task, which mapped one or more
of those queries to each column. For example:

**Query**
```yaml
no_zero_epoch_time: >
  SELECT CASE WHEN
                  count(*) = 0
  THEN 1 ELSE 0 END
  FROM $table
  WHERE $table.$column = to_timestamp('1970-01-01', 'YYYY-MM-DD')
```

_(By returning either 1 or 0 as Boolean proxies for every query, we can
use the same `assert` on every individual query.)_

**Assertion**
```yaml
task_name:
  source: NameOfSourceLuigiTask
  destination: table_name_in_destination (OPTIONAL)
  columns:
    - column_name:
        - no_zero_epoch_time
        - another_template_key

```

Then, use PyTest's parameterization functions to fetch the assertions file,
the query templates, shuffle them together into a set of valid SQL queries,
and let PyTest run all of them.

**Generate tests**
```python
def pytest_generate_tests(metafunc):
    argument_values = []
    assertions = None
    test_ids = []

    assertions_path = 'assertions/*.yaml'
    test_templates_path = 'templates/queries.yaml'

    with open(test_templates_path, 'r') as test_templates_file:
        templates_dict = yaml.safe_load(test_templates_file)

    for template_name, template_string in templates_dict.items():
        # cleaning up templates here allows users a free hand
        # with SQL formatting in test_templates.yaml without
        # making the output of a failed test a pain to copy/paste
        # if you're debugging w/ SQL.
        # regex finds all sets of whitespace chars between
        # non-whitespace chars larger than 1, substitutes 1 space
        templates_dict[template_name] = Template(
            re.sub(r'(\S)\s{2,}(\S)', r'\1 \2', template_string.strip()))

    for assertion_file_path in glob.glob(assertions_path):
        with open(assertion_file_path, 'r') as assertions_file:
            assertions_dict = yaml.safe_load(assertions_file)

        for task, assertions in assertions_dict.items():
            test_ids.append(task)

            # specifying destination table is optional in config
            # but required in test runs, so set it if not set in config
            if 'destination' not in assertions:
                assertions['destination'] = task

            argument_values.append(assertions)

    metafunc.parametrize('templates', (templates_dict,), ids=('at',))
    metafunc.parametrize('assertions', argument_values, ids=test_ids, scope='function')
```

**Run tests**
```python
def test_assertion(build, db, assertions, templates):
        import tasks

        queries = []
        column_names = []
        requirements = []

        destination_table = assertions['destination']

        task = getattr(tasks, assertions['source'])()
        results = build(task)
        assert results is True, 'Task {} failed'.format(assertions['source'])

        # TODO
        # for now we're assuming each task just has 1 data input
        # but in the future we'd want to create a data frame
        # for each data input (?)
        #
        # these next few lines will get a quick upper bound for the size
        # of the input file that we can use in some assertions
        input_data_paths = [input.path for input in task.requires().input()
                            if hasattr(input, 'path') and 'data' in input.path]
        with open(input_data_paths[0], 'r') as f:
            for i, l in enumerate(f):
                pass
        input_rows = i  # don't add one even though `i` is zero-indexed, b/c header row

        for column in assertions['columns']:
            for column_name, column_requirements in column.items():
                for requirement in column_requirements:
                    # quick and dirty way to batch queries w/o losing
                    # reference to column/requirement: 3 arrays w/
                    # matching indexes
                    column_names.append(column_name)
                    requirements.append(requirement)
                    queries.append(templates[requirement].safe_substitute(column=column_name,
                                                                          table=destination_table,
                                                                          source_row_count=input_rows))

        # stitch all queries into a single SQL statement to improve perf
        # TODO: add flag to run them individually to speed up debugging
        # if something's wrong w/ a query
        sql = 'SELECT (' + '),('.join(queries) + ')'
        results = db.execute(sql)[0]

        for index, result in enumerate(results):
            assert result == 1, 'Column `{}` violated assertion {} [query: {}]'.format(column_names[index],
                                                                                       requirements[index],
                                                                                       queries[index])
```

## Results

Pretty good! Compared to existing tests run under mutation using [mutmut](https://github.com/boxed/mutmut),
these tests killed mutants the existing tests missed, and in only one case
did the existing tests kill a mutant missed by this framework. Feel free
to try adapting this approach to your own data warehouse. It should work
in any language with decent testing support.

## What's with the repo name?

GitHubt suggested it and I thought sure, that sounds fun.

