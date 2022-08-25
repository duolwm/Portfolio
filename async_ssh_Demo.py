import click
import requests
import asyncio
import asyncssh
import logging.handlers

LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)-15s %(levelname)s %(processName)s-%(threadName)s,%(module)s,%(funcName)s: %(message)s'
logger = logging.getLogger(__name__)
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
formatter = logging.Formatter(LOG_FORMAT)
handler = logging.handlers.RotatingFileHandler('log/async_ssh.log', maxBytes=1024 * 1024, backupCount=2)
handler.setFormatter(formatter)
logger.addHandler(handler)


class MultiConnectDevice:
    def __init__(self, client_keys, source=None, target=None, cmd=None):
        self.api_url = 'dashboard_URL(port)'
        self.api_username = 'username'
        self.api_pwd = 'password'
        self.root_pwd = 'root_pwd'
        self.client_keys = client_keys
        self.source = source
        self.target = target
        self.cmd = cmd

    def get_frp_port(self) -> [list, list, dict]:
        r = requests.get(self.api_url, auth=(self.api_username, self.api_pwd))
        j = r.json()
        device_list_online = list()
        device_list_offline = list()
        remote_dict_online = dict()
        for i in j['proxies']:
            if i['status'] == 'online':
                device_list_online.append(i['name'].replace('.ssh', ''))
                remote_dict_online[i['name'].replace('.ssh', '')] = i['conf']['remote_port']
            elif i['status'] == 'offline':
                device_list_offline.append(i['name'].replace('.ssh', ''))
        device_list_online = sorted(device_list_online)
        return device_list_online, device_list_offline, remote_dict_online

    async def run_client(self, target_port: int, client_keys: str, source: str = None, target: str = None,
                         cmd: str = None) -> None:
        try:
            async with asyncssh.connect(
                    host='main_host_ip',
                    port=target_port,
                    username='username',
                    client_keys=client_keys,
                    known_hosts=None,
            ) as conn:
                if source and target:
                    await asyncssh.scp(source, (conn, target))
                root_cmd = f"""echo {self.root_pwd} | sudo -k -S sh -c '{cmd}'"""
                res = await conn.run(root_cmd)
                if res.exit_status != 0:
                    logger.error(f"""{target_port}：Fail""")
        except Exception as e:
            logger.error(e)

    def main(self):
        device_list_online, device_list_offline, remote_dict_online = self.get_frp_port()
        tasks = []
        for sname in device_list_online:
            tasks.append(self.run_client(target_port=remote_dict_online[sname], client_keys=self.client_keys, source=self.source,
                                target=self.target,
                                cmd=self.cmd))
        asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
        if device_list_offline:
            logging.info(f"""offline：{",".join(device_list_offline)}""")


if __name__ == '__main__':
    @click.command()
    @click.option('-cp', '--client_keys', default='./license_key', help='client keys path')
    @click.option('-s', '--source', help='config path')
    @click.option('-t', '--target', help='target folder')
    @click.option('-c', '--cmd', help='linux sh cmd')
    def run(client_keys: str='./id_rsa', source: str = None, target: str = None, cmd: str = None):
        multi_connect_device = MultiConnectDevice(client_keys=client_keys, source=source, target=target, cmd=cmd)
        multi_connect_device.main()

    run()