import zmq
import zmq.auth
import zmq.asyncio
import asyncio
from asyncio import ensure_future as create_task
import sys
import logging
import json
import argparse
from zmapi.zmq.utils import split_message, ident_to_str
from zmapi.logging import setup_root_logger
from zmapi.utils import check_missing, delayed
from zmapi.zmq import ReturningDealer
from zmapi.controller import MiddlewareCTL
from zmapi import fix
from zmapi import Publisher, Subscriber
import uuid
from sortedcontainers import SortedDict
from collections import defaultdict
from time import gmtime
from pprint import pprint, pformat
from datetime import datetime
from copy import deepcopy
from base64 import b64encode, b64decode


################################## CONSTANTS ##################################



################################### GLOBALS ###################################


L = logging.root

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.pub_handshake_done = False
g.pubs = {}


###############################################################################


class MyController(MiddlewareCTL):


    async def Logon(self, ident, msg_raw, msg):
        res_raw = (await self._dealer.send_recv_msg(msg_raw, ident=ident))[-1]
        res = json.loads(res_raw.decode())
        if res["Header"]["MsgType"] != fix.MsgType.Logon:
            return res_raw
        session_no = res["Body"].pop("SessionNo")
        g.pubs[session_no] = Publisher(g.sock_pub)
        res["Body"]["ZMSubSockTopic"] = str(session_no)
        if g.pub_handshake_done:
            return res
        L.info("performing handshake with pub socket ...")
        sub_ident = b64decode(res["Body"].pop("SubIdentity").encode())
        g.sock_sub.set(zmq.IDENTITY, sub_ident)
        g.sock_sub.connect(g.args.pub_addr_up)
        g.sock_sub.send(b"")
        await g.sock_sub.recv()
        L.info("handshake finished")
        g.pub_handshake_done = True
        create_task(g.sub.run())
        return res


###############################################################################


class MySubscriber(Subscriber):


    async def handle_one(self):
        if self._expected_session_id is None:
            self._expected_session_id = await self._get_session_id()
            L.debug(self._tag + "upstream session id: {}"
                    .format(self._expected_session_id))
        msg_parts = await self._sock_sub.recv_multipart()
        # print(msg_parts)
        # message must be the last part of the message
        msg = json.loads(msg_parts[-1].decode())
        topic = None
        if len(msg_parts) > 1:
            # topic must be the second last part of the message
            topic = msg_parts[-2].strip(b"\0").decode()
        state = self._state[topic]
        header = msg["Header"]
        seq_no = header.get("MsgSeqNum")
        if seq_no is None:
            await self._handle_msg_1(topic, msg)
            return
        if state["expected_seq_no"] is None:
            state["expected_seq_no"] = seq_no
        if seq_no == state["expected_seq_no"]:
            state["expected_seq_no"] += 1
            await self._handle_msg_1(topic, msg)
            return
        if seq_no < state["expected_seq_no"]:
            if header.get("PossDupFlag"):
                return
            L.info(self._tag + "MsgSeqNum smaller than expected")
            await self._on_seq_no_reset()
            if seq_no == 1:
                state["expected_seq_no"] = 2
                await self._handle_msg_1(topic, msg)
                return
            state["expected_seq_no"] = 1
        assert seq_no > state["expected_seq_no"], locals()
        start = state["expected_seq_no"]
        end = seq_no - 1
        state["expected_seq_no"] = seq_no + 1
        L.warning("{}gap detected: {}-{}".format(self._tag, start, end))
        if self._session_id:
            self.info(self._tag + "requesting gap fill ...")
            try:
                messages = await self._req_gap_fill(
                        start, end, topic)
            except Exception:
                L.error("error on Subscriber._req_gap_fill:")
            else:
                for t, m in zip(topics, messages):
                    await self._handle_msg_1(t, m)
        await self._handle_msg_1(topic, msg)


    async def _handle_msg_2(self, topic, msg):
        session_no = int(topic)
        await g.pubs[session_no].publish_msg(msg, topic.encode())


###############################################################################


def parse_args():
    desc = "portal middleware module"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("ctl_addr_up",
                        help="address of the upstream ctl socket")
    parser.add_argument("ctl_addr_down",
                        help="ctl socket binding address")
    parser.add_argument("pub_addr_up",
                        help="address of the upstream pub socket")
    parser.add_argument("pub_addr_down",
                        help="pub socket binding address")
    parser.add_argument("--key-file",
                        help="path to public key file "
                             "(default: dummy.key_public)")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()
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

    g.sock_deal = g.ctx.socket(zmq.DEALER)
    g.sock_deal.curve_publickey, g.sock_deal.curve_secretkey = \
            zmq.curve_keypair()
    g.sock_deal.curve_serverkey, _ = \
            zmq.auth.load_certificate(g.key_file)
    g.sock_deal.connect(args.ctl_addr_up)

    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr_down)

    g.sock_sub = g.ctx.socket(zmq.DEALER)
    g.sock_sub.curve_publickey, g.sock_sub.curve_secretkey = zmq.curve_keypair()
    g.sock_sub.curve_serverkey, _ = \
            zmq.auth.load_certificate(g.key_file)

    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.pub_addr_down)


def main():

    g.args = parse_args()
    setup_logging(g.args)
    init_zmq_sockets(g.args)
    g.ctl = MyController(g.sock_ctl, ReturningDealer(g.ctx, g.sock_deal))
    g.sub = MySubscriber(g.sock_sub, g.ctl._dealer, name="sub")
    tasks = [
        g.ctl.run(),
    ]
    tasks = [create_task(coro_obj) for coro_obj in tasks]

    try:
        g.loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        pass

    g.ctx.destroy()

if __name__ == "__main__":
    main()
