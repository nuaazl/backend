import asyncio
import re
import sys
from asyncio import wait_for
from typing import Optional, Tuple

import asyncssh
from asyncssh import SSHClientConnection, SSHClient, SSHClientSession, SSHClientChannel, SSHKey, \
    SSHClientConnectionOptions


class SSHUserOrPasswordError(Exception):
    ...

class SSHClientLostError(Exception):
    ...


class NoFTPSSHClientSession(SSHClientSession):
    """
    根据https://asyncssh.readthedocs.io/en/latest/#simple-client写法修改
    """

    def __init__(self):
        self._response: Optional[asyncio.Future] = None
        self.expect_end_flag: Optional[str] = None
        self.received_data: str = ""
        self.is_lost: bool = False  # session是否断开连接

    @property
    def response(self):
        return self._response

    @response.setter
    def response(self, value):
        if not isinstance(value, asyncio.Future):
            raise Exception("此处应该是一个asyncio下的Future")
        self._response = value

    def clear(self):
        self.response = None
        self.received_data = ""

    def data_received(self, data: str, datatype: asyncssh.DataType) -> None:
        print(data, end='')
        self.received_data += data
        if self.response is not None and self.expect_end_flag is not None:
            if self.received_data.endswith(self.expect_end_flag):
                self.response.set_result(self.received_data)
                del self._response
                self.clear()

    def connection_lost(self, exc) -> None:
        self.is_lost = True
        if exc:
            print('SSH session error: ' + str(exc), file=sys.stderr)


class NoFTPSSHClient(SSHClient):
    def connection_made(self, conn: asyncssh.SSHClientConnection) -> None:
        print(f'Connection made to {conn.get_extra_info("peername")[0]}.')

    def auth_completed(self) -> None:
        ...

    def validate_host_public_key(
            self, host: str, addr: str,
            port: int, key: SSHKey
    ) -> bool:
        """防止新的ip登录产生报警"""
        return True


class NoFTPAsyncSSH:

    def __init__(self, ip: str, username: str, password: str, port: int = 22):
        self.ip = ip
        self.username = username
        self.password = password
        self.port = port
        self.session: Optional[NoFTPSSHClientSession] = None
        self.chan: Optional[SSHClientChannel] = None
        self.conn: Optional[SSHClientConnection] = None

    def is_closed(self) -> bool:
        if self.chan is None:
            return True
        return self.chan.is_closing() or self.session.is_lost

    async def set_connection(self):
        try:
            conn, _ = await asyncssh.create_connection(
                NoFTPSSHClient,
                host=self.ip,
                port=self.port,
                username=self.username,
                password=self.password,
                options=SSHClientConnectionOptions(connect_timeout=3)
            )
            self.conn = conn
        except asyncssh.misc.PermissionDenied:
            raise SSHUserOrPasswordError("用户名或者密码错误！")

    async def set_session(self):
        """
        需要注意数据传输限制，以及term_size，如果term_size小会导致收到的结果奇奇怪怪！
        :return:
        """
        if self.conn is None:
            raise Exception("应该先调用set_connection")
        chan, session = await self.conn.create_session(
            NoFTPSSHClientSession, "",
            term_type='xterm-color', term_size=(4096, 48),
            errors="ignore"
        )
        self.chan = chan
        self.session = session

    async def set_end_content(self, content: Optional[str] = None):
        if content is None:
            if self.chan is None or self.is_closed():
                raise Exception("连接已经关闭了！")
            self.chan.write("\n")
            self.chan.write_eof()
            while True:
                content = self.session.received_data
                if content != "":
                    break
                await asyncio.sleep(0.1)
        self.session.expect_end_flag = content

    async def close(self):
        if self.chan:
            self.chan.close()
            await self.chan.wait_closed()
        if self.conn:
            self.conn.close()
            await self.conn.wait_closed()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def __aenter__(self):
        try:
            await self.set_connection()
            await self.set_session()
            await self.set_end_content()
        except Exception:
            await self.__aexit__(None, None, None)
            raise

    async def send_and_recv(
        self,
        command: str,
        timeout: float = 5,
        is_wait: bool = True,
        wait_seconds: float = 5
    ) -> Tuple[str, bool]:
        """
        交互式伪终端，要考虑多个协程共同使用的情况
        :param timeout: 命令执行超时时间
        :param wait_seconds: 如果等待，等待几秒
        :param is_wait: 如果阻塞是否等待命令执行完成
        :param command:
        :return:
        """
        # \x03是ctrl+c，打断执行时间较长，且未加&后台运行的命令，取消阻塞终端
        if self.session.response is not None and command != "\x03":
            if not is_wait:
                raise RuntimeError("当前ssh终端被占用，请稍后再执行命令")
            count = int(wait_seconds / 0.1)
            while count > 0:
                await asyncio.sleep(wait_seconds)
                if self.session.response is None:
                    break
                count -= 1
            else:
                raise TimeoutError("等待之前命令执行超时！")
        _last_command = command.split("\n")[-1]
        command = f"{command.strip()}\n"
        self.chan.write(command)
        fut = asyncio.Future()
        self.session.response = fut
        try:
            res = await wait_for(fut, timeout)
        except asyncio.TimeoutError:
            if self.session.is_lost:
                raise SSHClientLostError("ssh连接断开")
            else:
                raise
        # res需要去除颜色，和一些异常信息
        res_without_color = re.sub("\\x1b\[[\d;]*m", "", res)
        rec = res_without_color.strip().split("\r\n")

        # 因为接收到的内容里携带着发出去的命令，所以需要去掉相关信息
        for ind, r in enumerate(rec):
            if r.strip().endswith(_last_command):
                break
        else:
            return res_without_color, False
        if ind == len(rec) - 1:
            return "", True
        result = "\r\n".join(rec[ind:])
        return result, True
