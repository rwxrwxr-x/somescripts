
#!/usr/bin/env python3
"""
This script is used to import/export ci/cd variables. This is used in situations when moving a project.
Command structure:
  - `./variables.py export` or `./variable.py export` print ci/cd variables to stdout
  - `./variables.py import` or ./variables import` send environment variables from stdin
  - `./variables.py export | ./variables.py import` transfer variables
  - `./variables.py clear` clear all variables!
Arguments:
  - `command` import or export
Options:
  - `--env-file` file contains variables
  - `--gitlab-private-token` required
  - `--gitlab-url` optional, default `https://gitlab.com`
  - `--gitlab-group` required
  - `--gitlab-project` optional
"""
import argparse
import enum
import inspect
import json
import logging
import sys
import typing
import urllib.request
import urllib.parse
import urllib.error

logger = logging.getLogger(__name__)


class Command(enum.Enum):
    """Available commands"""
    cmd_import = 'import'
    cmd_export = 'export'
    cmd_clear = 'clear'

    def __str__(self):
        return self.value


class GitlabSettings:
    """Gitlab settings dataclass"""

    def __init__(self, *args, **kwargs):
        self.private_token: typing.Optional[str] = kwargs.get('private_token')
        self.url: typing.Optional[str] = kwargs.get('url')
        self.group: typing.Optional[str] = kwargs.get('group')
        self.project: typing.Optional[str] = kwargs.get('project')

        # Always remove trailing slash
        if self.project is not None:
            self.project = urllib.parse.quote_plus(self.project)

        self.url = self.url.rstrip('/') + '/api/v4'


class GitlabVariable:
    """Gitlab variable dataclass"""

    def __init__(self, *args, **kwargs):
        self.variable_type = kwargs.get('variable_type')
        self.key: str = kwargs.get('key')
        self.value: str = kwargs.get('value')
        self.protected: bool = kwargs.get('protected') or False
        self.masked: bool = kwargs.get('masked') or False
        self.environment_scope: str = kwargs.get('environment_scope')


class GitlabClient:
    """Gitlab api client"""

    def __init__(self, settings: GitlabSettings):
        self.settings = settings

    def call(self, method: str, path: str, data=None):
        url = f'{self.settings.url}/{path.lstrip("/")}'
        headers = {
            'PRIVATE-TOKEN': self.settings.private_token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        logger.info('request gitlab %s %s', method, path)
        if data is not None:
            data = urllib.parse.urlencode(data).encode()
        request = urllib.request.Request(url, method=method, headers=headers, data=data)
        response = urllib.request.urlopen(request)
        raw_data = response.read()
        if not raw_data:
            return None, None
        data = json.loads(raw_data)
        return data, response.headers

    def get_variables(self) -> typing.List[typing.Dict[str, any]]:
        path = f'projects/{self.settings.project}/variables' if self.settings.project \
            else f'groups/{self.settings.group}/variables'
        page = 1
        per_page = 100
        variables = []
        while True:
            data, headers = self.call('GET', path + '?' + urllib.parse.urlencode({'page': page, 'per_page': per_page}))
            variables.extend(data)
            total_pages = int(headers.get('X-Total-Pages'))
            if page == total_pages:
                break
            page += 1
        return variables

    def get_variable(self, gitlab_variable: GitlabVariable):
        path = f'projects/{self.settings.project}/variables/{gitlab_variable.key}' if self.settings.project \
            else f'groups/{self.settings.group}/variables/{gitlab_variable.key}'
        path += '?filter[environment_scope]=' + urllib.parse.quote_plus(gitlab_variable.environment_scope)
        data, _ = self.call('GET', path)
        return data

    def update_variable(self, gitlab_variable: GitlabVariable):
        path = f'projects/{self.settings.project}/variables/{gitlab_variable.key}' if self.settings.project \
            else f'groups/{self.settings.group}/variables/{gitlab_variable.key}'
        path += '?filter[environment_scope]=' + urllib.parse.quote_plus(gitlab_variable.environment_scope)
        data = {
            k: v
            for k, v in gitlab_variable.__dict__.items()
            if k in ['value', 'variable_type', 'environment_scope'] or
               (k in ['protected', 'masked'] and v is True)
        }
        data, _ = self.call('PUT', path, data)
        return data

    def create_variable(self, gitlab_variable: GitlabVariable):
        path = f'projects/{self.settings.project}/variables' if self.settings.project \
            else f'groups/{self.settings.group}/variables'
        data = {
            k: v
            for k, v in gitlab_variable.__dict__.items()
            if k in ['key', 'value', 'variable_type', 'environment_scope'] or
               (k in ['protected', 'masked'] and v is True)
        }
        data, _ = self.call('POST', path, data)
        return data

    def delete_variable(self, gitlab_variable: GitlabVariable):
        path = f'projects/{self.settings.project}/variables/{gitlab_variable.key}' if self.settings.project \
            else f'groups/{self.settings.group}/variables/{gitlab_variable.key}'
        path += '?filter[environment_scope]=' + urllib.parse.quote_plus(gitlab_variable.environment_scope)
        data, _ = self.call('DELETE', path)
        return data

    def upsert_variable(self, gitlab_variable: GitlabVariable):
        try:
            data = self.get_variable(gitlab_variable)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
            data = None
        if data is not None:
            self.update_variable(gitlab_variable)
        else:
            self.create_variable(gitlab_variable)


def cmd_import(gitlab: GitlabClient, gitlab_variables: typing.List[GitlabVariable]):
    """Import command"""
    for item in gitlab_variables:
        logger.info('import variable %s environment_scope %s', item.key, item.environment_scope)
        gitlab.upsert_variable(item)


def cmd_export(gitlab: GitlabClient) -> typing.List[GitlabVariable]:
    """Export command"""
    data = gitlab.get_variables()
    return [
        GitlabVariable(**item)
        for item in data
    ]


def cmd_clear(gitlab: GitlabClient):
    print('Are you sure clear variables in gitlab? [No]')
    answer = input()
    if answer.lower() == 'yes':
        variables = [GitlabVariable(**item) for item in gitlab.get_variables()]
        for variable in variables:
            try:
                gitlab.delete_variable(variable)
            except urllib.error.HTTPError as e:
                logger.exception(e)


def param_gitlab(args) -> GitlabClient:
    """Gitlab settings builder"""
    return GitlabClient(GitlabSettings(
        private_token=args.gitlab_private_token,
        url=args.gitlab_url,
        group=args.gitlab_group,
        project=args.gitlab_project,
    ))


def param_gitlab_variables(args) -> typing.List[GitlabVariable]:
    """Gitlab variables builder"""
    data = json.load(args.input)
    if not isinstance(data, list):
        return []
    else:
        return [GitlabVariable(**item) for item in data]


def main(argv: list):
    """Main/entrypoint"""
    args_parser = argparse.ArgumentParser(prog='./variables.py', description='Gitlab Variables')
    args_parser.add_argument('command', type=Command, choices=list(Command), help='Command name')
    args_parser.add_argument('--input', type=argparse.FileType('r'), required=False, default=sys.stdin,
                             help='Input file')
    args_parser.add_argument('--output', type=argparse.FileType('w'), required=False, default=sys.stdout,
                             help='Output file')
    args_parser.add_argument('--gitlab-private-token', type=str, required=True, action='store',
                             help='Gitlab private token')
    args_parser.add_argument('--gitlab-url', type=str, required=False, default='https://gitlab.com/',
                             action='store', help='Gitlab url address')
    args_parser.add_argument('--gitlab-group', type=str, required=False, action='store', help='Gitlab group ID or SLUG')
    args_parser.add_argument('--gitlab-project', type=str, required=False, action='store',
                             help='Gitlab project ID or SLUG')
    args_parser.add_argument('--verbose', action='store_true', default=False, help='Verbose log')
    args = args_parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    routing = {cmd.value: globals()[cmd.name] for cmd in Command}
    param_routing = {p: globals()[f'param_{p}'] for p in {'gitlab', 'gitlab_variables'}}
    handler = routing.get(args.command.value)
    if handler is None:
        raise SystemExit(f'Handler {args.command} not defined')
    elif not callable(handler):
        raise SystemExit(f'Handler {args.command} incorrect defined')
    handler_kwargs = {}
    for param_type in inspect.signature(handler).parameters.values():
        param_getter = param_routing.get(param_type.name)
        if param_getter is None:
            raise SystemExit(f'Handler param {param_type.name} not define getter func')
        elif not callable(param_getter):
            raise SystemExit(f'Handler param {param_type.name} not callable getter func')
        handler_kwargs[param_type.name] = param_getter(args)
    result = handler(**handler_kwargs)
    if result is None:
        pass
    elif isinstance(result, str):
        args.output.write(result)
    else:
        args.output.write(json.dumps(
            result,
            ensure_ascii=False,
            indent='  ',
            default=lambda obj: obj.__dict__ if isinstance(obj, (GitlabVariable,)) else str(obj),
        ))


if __name__ == '__main__':
    main(sys.argv)