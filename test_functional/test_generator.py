from string import Template

import glob
import re
import yaml


# TODO: add docstring
def pytest_generate_tests(metafunc):
    argument_values = []
    assertions = None
    test_ids = []

    assertions_path = 'assertions/*.yaml'
    test_templates_path = 'templates/queries.yaml'

    # TODO: check for existence of test template file
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
