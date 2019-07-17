import zmq
import zmq.asyncio
import zmq.auth.asyncio
import asyncio
from asyncio import ensure_future as create_task
import sys
import os
import logging
import json
import argparse
import subprocess
from zmapi.zmq.utils import split_message, ident_to_str
from zmapi.logging import setup_root_logger
from zmapi.utils import check_missing, delayed, get_timestamp, wipe_dir
from zmapi.zmq import ReturningDealer
from zmapi.controller import Controller
from zmapi import Publisher
from zmapi.exceptions import *
import uuid
from sortedcontainers import SortedDict
from collections import defaultdict
from time import gmtime
from pprint import pprint, pformat
from datetime import datetime
from copy import deepcopy
from base64 import b64encode, b64decode
from tempfile import TemporaryDirectory


################################## CONSTANTS ##################################



################################### GLOBALS ###################################


L = logging.root

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.disable_auth = False
g.idents = {}

def create_first_level_ident_dict():
    return {
        "handshake_done": False,
        "publisher": None,
    }
g.first_level_idents = defaultdict(create_first_level_ident_dict)

g.session_no = 0


###############################################################################


def get_credentials(msg):
    if g.disable_auth:
        # permit all
        return {}
    body = msg["Body"]
    username = body["Username"]
    password = body["Password"]
    # TODO: check against auth credential file to see if user/password is ok
    #       and return the relevant permission specification


async def send_recv_command(dealer, msg_type, **kwargs):
    body = kwargs.get("body", {})
    ident = kwargs.get("ident", None)
    timeout = kwargs.get("timeout", None)
    endpoint = kwargs.get("endpoint", None)
    check_error = kwargs.get("check_error", False)
    msg = {}
    msg["Header"] = header = {}
    header["MsgType"] = msg_type
    if endpoint:
        header["ZMEndpoint"] = endpoint
    msg["Body"] = body
    msg_bytes = (" " + json.dumps(msg)).encode()
    msg_parts = await dealer.send_recv_msg(
            msg_bytes, ident=ident, timeout=timeout)
    if not msg_parts:
        return
    msg = json.loads(msg_parts[-1].decode())
    if check_error:
        check_if_error(msg)
    return msg


def terminate_session(ident):
    ident_key = tuple(ident)
    ident_str = ident_to_str(ident)
    d = g.idents.pop(ident_key)
    log_prefix = "[sessionizer-{session_no}] ".format(**d)
    d["dealer"].close()
    d["sock_sub"].close()
    p = d["sessionizer_process"]
    p.kill()
    L.debug(log_prefix + "terminated")


###############################################################################


class MyController(Controller):


    def __init__(self, sock_dn):
        super().__init__(sock_dn)


    async def _start_new_session(self, ident):

        ident_key = tuple(ident)
        ident_str = ident_to_str(ident)
        d = g.idents[ident_key]
        log_prefix = "[sessionizer-{session_no}] ".format(**d)
        L.debug(log_prefix + "starting ...")
        sessionizer_name = "sessionizer-{session_no}".format(**d)
        sock_fn = f"/tmp/ctl-{sessionizer_name}"
        try:
            os.remove(sock_fn)
        except FileNotFoundError:
            pass
        ctl_addr_down = f"ipc://{sock_fn}"
        sock_fn = f"/tmp/pub-{sessionizer_name}"
        try:
            os.remove(sock_fn)
        except FileNotFoundError:
            pass
        pub_addr_down = f"ipc://{sock_fn}"
        cmd = [
            "python",
            "../sessionizer/sessionizer.py",
            g.args.ctl_addr_up,
            ctl_addr_down,
            g.args.pub_addr_up,
            pub_addr_down,
            "--hot-plug",
            "--log-level",
            "DEBUG"
        ]
        output_file = open(f"/tmp/{sessionizer_name}.log", "w")
        p = subprocess.Popen(cmd, stdout=output_file, stderr=output_file)

        sock_deal = g.ctx.socket(zmq.DEALER)
        sock_deal.setsockopt_string(zmq.IDENTITY, "gateway")
        sock_deal.connect(ctl_addr_down)
        dealer = ReturningDealer(g.ctx, sock_deal)
        create_task(dealer.run())
        sock_sub = g.ctx.socket(zmq.SUB)
        sock_sub.connect(pub_addr_down)
        sock_sub.subscribe(b"")

        L.debug(log_prefix + "waiting for message from pub ...")
        res = await sock_sub.recv_string()
        res = json.loads(res)
        assert res["Header"]["MsgType"] == fix.MsgType.Heartbeat, pformat(res)
        L.debug(f"{log_prefix}message received from pub "
                f'(ping no: {res["Body"]["_PingNum"]})')
        body = {"_PubReceived": True}
        res = await send_recv_command(
                dealer, fix.MsgType.TestRequest, body=body)
        poller = zmq.asyncio.Poller()
        poller.register(sock_sub)
        no_purged = 0
        while True:
            res = await poller.poll(100)
            if not res:
                break
            res = await sock_sub.recv_string()
            res = json.loads(res)
            assert res["Header"]["MsgType"] == fix.MsgType.Heartbeat, \
                    pformat(res)
            no_purged += 1
        L.debug(f"{log_prefix}{no_purged} hot plug pings purged")

        # # relief for cold start problem
        # received = False
        # async def requester():
        #     nonlocal received
        #     req_no = 1
        #     while not received:
        #         L.debug(log_prefix + f"requesting TestRequest {req_no} ...")
        #         body = {
        #             "TestReqID": req_no,
        #             "ZMSendToPub": True,
        #         }
        #         req_no += 1
        #         await send_recv_command(
        #                 dealer,
        #                 fix.MsgType.TestRequest,
        #                 body=body)
        #         await asyncio.sleep(0.01)
        # create_task(requester())
        # L.debug(log_prefix + "waiting for message from pub ...")
        # await sock_sub.recv_multipart()
        # received = True
        # L.debug(log_prefix + "message received from pub")

        d["dealer"] = dealer
        d["sock_sub"] = sock_sub
        d["sessionizer_process"] = p
        create_task(pub_handler(ident))
        L.debug(log_prefix + "{} started, pid: {}"
                .format(sessionizer_name, p.pid))


    async def Logon(self, ident, msg_raw, msg):
        ident_key = tuple(ident)
        if ident_key in g.idents:
            raise RejectException("already logged in")
        g.idents[ident_key] = d = get_credentials(msg)
        d["session_no"] = g.session_no
        g.session_no += 1
        await self._start_new_session(ident)
        d["health"] = g.timeout_heartbeats
        res = {}
        res["Header"] = {"MsgType": fix.MsgType.Logon}
        res["Body"] = body = {}
        body["SessionNo"] = d["session_no"]
        lvl1_ident = ident[0]
        if lvl1_ident not in g.first_level_idents:
            fl_d = g.first_level_idents[lvl1_ident]
            fl_d["publisher"] = MyPublisher(g.sock_pub, lvl1_ident)
            body["SubIdentity"] = b64encode(lvl1_ident).decode()
        return res


    async def Logout(self, ident, msg_raw, msg):
        ident_key = tuple(ident)
        if ident_key not in g.idents:
            raise RejectException("not logged in")
        terminate_session(ident)
        res = {}
        res["Header"] = {}
        res["MsgType"] = fix.MsgType.Logout
        res["Body"] = {}
        return res


    async def ResendRequest(self, ident, msg_raw, msg):
        lvl1_ident = ident[0]
        # if len(ident) != 1:
        #     raise RejectException(
        #             "no permissions",
        #             fix.ZMRejectReason.InsufficientPermissions)
        if lvl1_ident not in g.first_level_idents:
            raise RejectException("no subscription")
        fl_d = g.first_level_idents[lvl1_ident]
        body = msg["Body"]
        start = body["BeginSeqNo"]
        end = body["EndSeqNo"]
        send_to_pub = body.get("ZMSendToPub", False)
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMResendRequestResponse
        res["Body"] = body = {}
        res["ZMSessionID"] = self._session_id
        if send_to_pub:
            await fl_d["publisher"].republish(start, end)
            body["Text"] = "republished to pub"
            return res
        _, topics, messages = \
                zip(*fl_d["publisher"].fetch_messages(start, end))
        body["ZMNoPubMessages"] = group = []
        for msg in messages:
            msg = b64encode((" " + json.dumps(msg)).encode()).decode()
            group.append(msg)
        if topics[0] is not None:  # if first topic is None, all of them are
            body["ZMNoSubscriberTopics"] = group = []
            for topic in topics:
                group.append(b64encode(topic).decode())
        return res


    def _handle_get_session_id(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = {"MsgType": fix.MsgType.ZMGetSessionIDResponse}
        res["Body"] = {"ZMSessionID": self.session_id}
        return res


    async def _handle_msg_2(self, ident, msg_raw, msg, msg_type):
        if msg_type == fix.MsgType.ZMGetSessionID:
            return self._handle_get_session_id(ident, msg_raw, msg)
        ident_key = tuple(ident)
        if ident_key in g.idents:
            g.idents[ident_key]["health"] = g.timeout_heartbeats
        f = self._commands.get(msg_type)
        if f:
            return await f(self, ident, msg_raw, msg)
        if ident_key not in g.idents:
            # TODO: allow only ctl type messages if disable auth
            raise NotImplementedError("anonymous req not implemented")
        d = g.idents[ident_key]
        # TODO: check permissions here
        return (await d["dealer"].send_recv_msg(msg_raw))[-1]


###############################################################################


class MyPublisher(Publisher):


    def __init__(self, sock, lvl1_ident):
        self._lvl1_ident = lvl1_ident
        msg_cache_dir = os.path.join(g.temp_dir.name,
                                     b64encode(lvl1_ident).decode())
        super().__init__(sock,
                         msg_cache_dir=msg_cache_dir,
                         max_buffer_bytes=500)


    async def _send_msg(self, topic : bytes, msg : bytes):
        if topic is None:
            msg_parts = [self._lvl1_ident, b"", msg]
        else:
            msg_parts = [self._lvl1_ident, b"", topic, msg]
        await self._sock.send_multipart(msg_parts)


###############################################################################


async def pub_handler(ident):

    lvl1_ident = ident[0]
    pub = g.first_level_idents[lvl1_ident]["publisher"]
    ident_key = tuple(ident)
    d = g.idents[ident_key]
    log_prefix = "[pub-handler-{}] ".format(d["session_no"])
    topic = str(d["session_no"]).encode() + b"\0"
    sock = d["sock_sub"]
    L.debug(log_prefix + "running ...")

    while ident_key in g.idents:

        msg_parts = await sock.recv_multipart()
        # print(msg_parts)
        msg = json.loads(msg_parts[-1].decode())

        # skip pub connection establishment heartbeats
        # try:
        #     if msg["Header"]["MsgType"] == fix.MsgType.Heartbeat \
        #             and msg["Body"].get("TestReqID") == "INITIAL_PING":
        #         continue
        # except:
        #     continue

        if not g.first_level_idents[lvl1_ident]["handshake_done"]:
            L.warning(log_prefix + \
                      "lvl1 identity not yet connected, skipping msg ...")
            continue

        # L.debug(log_prefix + "received msg")
        await pub.publish_msg(msg, topic)

    L.debug(log_prefix + "exited")


###############################################################################


async def pub_router_handshaker():
    log_prefix = "[handshaker] "
    L.debug(log_prefix + "running ...")
    while True:
        msg_parts = await g.sock_pub.recv_multipart()
        lvl1_ident = msg_parts[0]
        if lvl1_ident not in g.first_level_idents:
            L.warning(log_prefix + "unknown ident, ignored")
            continue
        if g.first_level_idents[lvl1_ident]["handshake_done"]:
            L.warning(log_prefix + "handshake requested again, ignored")
        g.sock_pub.send_multipart(msg_parts)
        g.first_level_idents[lvl1_ident]["handshake_done"] = True
        L.info(log_prefix + "handshake accepted from: '{}'"
               .format(ident_to_str([lvl1_ident])))


###############################################################################


async def reaper():
    L.debug("reaper running ...")
    while True:
        for ident in list(g.idents.keys()):
            d = g.idents[ident]
            if "health" not in d:
                L.debug("ident data has a problem:\n{}".format(pformat(d)))
            d["health"] -= 1
            if d["health"] <= 0:
                ident_str = ident_to_str(ident)
                log_prefix = "[sessionizer-{session_no}] ".format(**d)
                L.info(log_prefix + "timed out, terminating ...")
                terminate_session(ident)
        await asyncio.sleep(g.heartbeat_interval)


###############################################################################


def parse_args():

    desc = "gateway middleware module"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("ctl_addr_up",
                        help="address of the upstream ctl socket")
    parser.add_argument("ctl_addr_down",
                        help="ctl socket binding address")
    parser.add_argument("pub_addr_up",
                        help="address of the upstream pub socket")
    parser.add_argument("pub_addr_down",
                        help="pub socket binding address")
    parser.add_argument("--disable-auth", action="store_true",
                        help="disable authentication")
    parser.add_argument("--timeout-heartbeats", type=int, default=3,
                        help="maximum heartbeats to skip before timeout")
    parser.add_argument("--heartbeat-interval", type=float, default=60,
                        help="heartbeat interval in seconds")
    parser.add_argument("--key-file",
                        help="path to private key file "
                             "(default: dummy.key_secret)")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()

    g.disable_auth = args.disable_auth
    g.timeout_heartbeats = args.timeout_heartbeats
    g.heartbeat_interval = args.heartbeat_interval
    if args.key_file:
        g.key_file = args.key_file
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        g.key_file = os.path.join(script_dir, "dummy.key_secret")
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args


def setup_logging(args):
    setup_root_logger(args.log_level)


def init_zmq_sockets(args):

    g.auth = zmq.auth.asyncio.AsyncioAuthenticator()
    g.auth.start()
    g.auth.allow("127.0.0.1")
    g.auth.configure_curve(location=zmq.auth.CURVE_ALLOW_ANY)

    server_public, server_secret = \
            zmq.auth.load_certificate(g.key_file)

    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.curve_secretkey = server_secret
    g.sock_ctl.curve_publickey = server_public
    g.sock_ctl.curve_server = True
    g.sock_ctl.bind(args.ctl_addr_down)

    g.sock_pub = g.ctx.socket(zmq.ROUTER)
    g.sock_pub.curve_secretkey = server_secret
    g.sock_pub.curve_publickey = server_public
    g.sock_pub.curve_server = True
    g.sock_pub.bind(args.pub_addr_down)


def main():

    g.args = parse_args()
    setup_logging(g.args)
    # wipe_dir("msg_cache")
    init_zmq_sockets(g.args)
    g.ctl = MyController(g.sock_ctl)
    tasks = [
        g.ctl.run(),
        pub_router_handshaker(),
        reaper(),
    ]
    tasks = [create_task(coro_obj) for coro_obj in tasks]
    g.temp_dir = TemporaryDirectory(prefix="zmapi_gateway_")


    try:
        g.loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        pass

    for ident in list(g.idents.keys()):
        terminate_session(ident)

    g.ctx.destroy()
    # TODO: remove temp dir ...


if __name__ == "__main__":
    main()
