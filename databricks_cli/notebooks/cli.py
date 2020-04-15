import subprocess

import click

from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, loadingbar
from databricks_cli.version import print_version_callback, version
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.configure.provider import get_config
from databricks_cli.workspace.api import WorkspaceApi


def _get_repo_path_and_name():
    path = subprocess.check_output(["git", "rev-parse", "--show-toplevel"])
    try:
        path = path.decode('utf-8')
    except AttributeError:
        pass
    path = path.strip()
    name = path.split("/")[-1]
    return path, name


def _get_local_and_remote_folders(alt_remote_folder=None):
    repo_path, repo_name = _get_repo_path_and_name()
    local = '{}/notebooks'.format(repo_path)
    username = get_config().username
    if alt_remote_folder is not None:
        remote = alt_remote_folder
    else:
        remote = '/Users/{}/{}'.format(username, repo_name)
    return local, remote


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Pull notebooks from Databricks into a local git repo.')
@click.option('--folder', help='Specify an alternative remote folder.')
@click.option('--verbose', is_flag=True, default=False,
              help='Show extra information.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def pull(api_client, folder, verbose):
    """
    Pull notebooks from Databricks into a local git repo.
    """
    local_folder, remote_folder = _get_local_and_remote_folders(folder)
    workspace = WorkspaceApi(api_client)

    def work():
        workspace.export_workspace_dir(remote_folder, local_folder, True,
                                       verbose=verbose)
    if not verbose:
        with loadingbar(msg="Pulling from {}".format(remote_folder),
                        width=10, fill_char="o", interval=.25):
            work()
    else:
        work()


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Push notebooks into Databricks from a local git repo.')
@click.option('--folder', help='Specify an alternative remote folder.')
@click.option('--verbose', is_flag=True, default=False,
              help='Show extra information.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def push(api_client, folder, verbose):
    """
    Push notebooks into Databricks from a local git repo.
    """
    local_folder, remote_folder = _get_local_and_remote_folders(folder)
    workspace = WorkspaceApi(api_client)

    def work():
        workspace.import_workspace_dir(local_folder, remote_folder,
                                       True, False, verbose=verbose)
    if not verbose:
        with loadingbar(msg="Pushing to {}".format(remote_folder), width=10,
                        fill_char="o", interval=.25):
            work()
    else:
        work()


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to sync Databricks notebooks in git.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
def notebooks_group():
    """
    Utility to sync Databricks notebooks in git.
    """
    pass


notebooks_group.add_command(pull, name='pull')
notebooks_group.add_command(push, name='push')
