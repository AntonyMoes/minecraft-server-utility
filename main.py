from asyncio import get_event_loop, AbstractEventLoop, sleep
import aiohttp
import json
from dataclasses import dataclass
from dacite import from_dict, exceptions
from contextlib import ExitStack
from paramiko import SSHClient, SSHException, AutoAddPolicy
from typing import NewType, Optional
from scp import SCPClient, SCPException
from datetime import datetime
import os
import sys

DEFAULT_CONFIG_FILE = 'config.json'
ERROR_CODE = 1
SERVER_ENCODING = 'utf-8'

Warning = NewType('Warning', str)
Error = NewType('Error', str)


@dataclass
class Mention:
    id: str
    is_role: bool = False

    def __str__(self) -> str:
        return f'<@{"&" if self.is_role else ""}{self.id}>'


@dataclass
class Config:
    iteration_time: int
    error_iteration_time: int
    webhook: str
    server_host: str
    server_user: str
    server_use_host_keys: bool
    server_password: str
    server_directory: str
    server_before_save_command: str
    server_after_save_command: str
    backup_name_prefix: str
    backup_directory: str
    backup_allowed_gigabytes: float
    backup_warning_ratio: float
    warning_mentions: list[Mention]
    error_mentions: list[Mention]


def get_command_outputs(ssh: SSHClient, command: str) -> (str, str):
    _, stdout, stderr = ssh.exec_command(command)
    return str(stdout.read().decode(SERVER_ENCODING)), str(stderr.read().decode(SERVER_ENCODING))


async def backup(config: Config) -> (str, Optional[Error]):
    with ExitStack() as stack:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.load_system_host_keys()
        stack.callback(ssh.close)

        password = config.server_password if not config.server_use_host_keys else None
        try:
            ssh.connect(hostname=config.server_host, username=config.server_user, password=password,
                        disabled_algorithms={'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']})
        except (OSError, SSHException) as e:
            return '', Error(f'Error while connecting to {config.server_user}@{config.server_host}: {e}')

        cd = f'cd {config.server_directory}/../ && ls -la'
        out, err = get_command_outputs(ssh, cd)
        if len(err) > 0:
            return '', Error(f'Could not open server directory "{config.server_directory}": {err}')

        archive_name = f'{config.backup_name_prefix}-{datetime.now().strftime("%Y-%m-%d_%H_%M")}.tar.gz'
        stack.callback(lambda: ssh.exec_command(f'{cd} && rm {archive_name}'))
        if len(config.server_before_save_command) > 0:
            _, err = get_command_outputs(ssh, f'{cd} && {config.server_before_save_command}')
            if len(err) > 0:
                return '', Error(f'Error while executing before_save_command "{config.server_before_save_command}": {err}')

        directory_name = os.path.normpath(config.server_directory)
        _, err = get_command_outputs(ssh, f'{cd} && tar -czf {archive_name} {directory_name}')
        if len(err) > 0:
            return '', Error(f'Could not compress backup archive: {err}')

        if len(config.server_after_save_command) > 0:
            _, err = get_command_outputs(ssh, f'{cd} && {config.server_after_save_command}')
            if len(err) > 0:
                return '', Error(f'Error while executing after_save_command "{config.server_after_save_command}": {err}')

        scp = SCPClient(ssh.get_transport())
        stack.callback(scp.close)

        try:
            scp.get(f'{config.server_directory}/{archive_name}', local_path=config.backup_directory)
        except (SCPException, OSError) as e:
            return '', Error(f'Could not copy archive to the local directory {config.backup_directory}: {e}')

    return archive_name, None


def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size


def check_local(config: Config) -> (Optional[Warning], Optional[Error]):
    try:
        size = get_size(config.backup_directory)
        gb_size = size / (1024 ** 3)
    except OSError as e:
        return None, Error(f'Could not read backup directory: {e}')

    ratio = gb_size / config.backup_allowed_gigabytes
    if ratio >= 1:
        return None, Error(f'Backup directory has reached the maximum size of {config.backup_allowed_gigabytes} GB')

    warning = None
    if ratio > config.backup_warning_ratio:
        warning = f'Backup folder has ratio of {round(ratio, 3)} of the maximum size of {config.backup_allowed_gigabytes} GB'

    return warning, None


async def notify(message: str, webhook: str):
    print(f"LOG: {message}")
    if len(webhook) == 0:
        return

    async with aiohttp.ClientSession() as session:
        async with session.post(webhook, json={'content': message}) as response:
            resp = await response.read()
            print(resp)


def get_mentions(mentions: list[Mention]) -> str:
    if len(mentions) == 0:
        return ''

    return '\n' + str.join(', ', map(str, mentions))


async def main_routine(config: Config):
    await notify('===========\n**Bot started**\n===========', config.webhook)

    while True:
        archive_name = ''

        warning, error = check_local(config)
        if error is None:
            archive_name, error = await backup(config)

        if error is None:
            message = f'Successful backup! Archive: `{archive_name}`'
        else:
            message = f'**ERROR**{get_mentions(config.error_mentions)}\n{error}'

        if warning is not None:
            message += f'\n\n**WARNING**{get_mentions(config.warning_mentions)}\n{warning}'

        await notify(message, config.webhook)
        await sleep(config.iteration_time if error is None else config.error_iteration_time)


def main():
    config_file = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CONFIG_FILE

    try:
        with open(config_file) as file:
            config_dict = json.load(file)
    except OSError:
        print(f'Could not open config file "{config_file}"')
        exit(ERROR_CODE)
    except json.JSONDecodeError as e:
        print(f'Config file invalid: {e}')
        exit(ERROR_CODE)

    try:
        config = from_dict(Config, config_dict)
    except exceptions.DaciteError as e:
        print(e)
        exit(ERROR_CODE)
    except Exception as e:
        print(type(e))
        print(e)
        exit(ERROR_CODE)

    loop: AbstractEventLoop = get_event_loop()
    loop.run_until_complete(main_routine(config))


if __name__ == '__main__':
    main()
